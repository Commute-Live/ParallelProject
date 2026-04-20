#!/usr/bin/env python3
"""
Download GTFS feeds declared in config/feeds.json.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
import shutil
import sys
import urllib.request
import urllib.error
import zipfile
from pathlib import Path
import urllib.parse
from urllib.parse import urlparse

ROOT_DIR = Path(__file__).resolve().parent.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default=str(ROOT_DIR / "config" / "feeds.json"),
        help="Path to the feed configuration JSON",
    )
    parser.add_argument(
        "--raw-dir",
        default=str(ROOT_DIR / "data" / "raw"),
        help="Directory where raw zip files and extracted contents should be stored",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download feeds even if the zip already exists",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Download the zip files without extracting them",
    )
    parser.add_argument(
        "--archive-limit",
        type=int,
        default=0,
        help="Limit the number of archived snapshots downloaded per archive-backed feed (0 means all)",
    )
    parser.add_argument(
        "--archive-since",
        help="Only download archive snapshots on or after this date (YYYY-MM-DD or YYYYMMDD)",
    )
    return parser.parse_args()


def load_config(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "parallel-transit-project/1.0"},
    )
    with urllib.request.urlopen(request) as response, destination.open("wb") as handle:
        shutil.copyfileobj(response, handle)


def safe_download_file(url: str, destination: Path) -> bool:
    try:
        download_file(url, destination)
        return True
    except (urllib.error.URLError, TimeoutError):
        if destination.exists():
            destination.unlink()
        return False


def fetch_json(url: str) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "parallel-transit-project/1.0",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def fetch_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "parallel-transit-project/1.0"},
    )
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8")


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract_dir)


def parse_archive_since(raw_value: str | None) -> dt.date | None:
    if raw_value is None:
        return None

    normalized = raw_value.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    raise ValueError(f"Invalid --archive-since date: {raw_value}")


def parse_date_candidate(raw_value: str | None) -> dt.date | None:
    if raw_value is None:
        return None

    normalized = raw_value.strip()
    if not normalized:
        return None

    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return dt.datetime.strptime(normalized, fmt).date()
        except ValueError:
            continue

    return None


def basename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    return name or "feed.zip"


def archive_dest_name(snapshot: dict[str, str], fallback_index: int) -> str:
    source_date = snapshot.get("service_date") or snapshot.get("published_at") or ""
    normalized_date = re.sub(r"[^0-9]", "", source_date) or f"{fallback_index:04d}"
    filename = snapshot["filename"]
    return f"{normalized_date}_{filename}"


def list_mbta_archive_snapshots(archive_cfg: dict[str, object]) -> list[dict[str, str]]:
    index_url = str(archive_cfg.get("index_url", "https://cdn.mbta.com/archive/archived_feeds.txt"))
    rows = list(csv.DictReader(fetch_text(index_url).splitlines()))
    snapshots: list[dict[str, str]] = []

    for row in rows:
        archive_url = (row.get("archive_url") or "").strip()
        if not archive_url:
            continue
        snapshots.append(
            {
                "download_url": archive_url,
                "filename": basename_from_url(archive_url),
                "service_date": (row.get("feed_start_date") or "").strip(),
            }
        )

    snapshots.sort(key=lambda item: item["service_date"])
    return snapshots


def list_github_release_snapshots(archive_cfg: dict[str, object]) -> list[dict[str, str]]:
    repo = archive_cfg.get("repo")
    if not repo:
        raise ValueError("github_releases archive requires a repo field")

    asset_pattern = archive_cfg.get("asset_pattern", r"\.zip$")
    compiled_pattern = re.compile(str(asset_pattern))
    releases_url = f"https://api.github.com/repos/{repo}/releases?per_page=100"
    releases = fetch_json(releases_url)
    if not isinstance(releases, list):
        raise ValueError(f"Unexpected GitHub releases response for {repo}")

    snapshots: list[dict[str, str]] = []
    for release in releases:
        if not isinstance(release, dict):
            continue
        published_at = str(release.get("published_at") or "")
        assets = release.get("assets") or []
        if not isinstance(assets, list):
            continue
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            name = str(asset.get("name") or "")
            download_url = str(asset.get("browser_download_url") or "")
            if not name or not download_url or not compiled_pattern.search(name):
                continue
            snapshots.append(
                {
                    "download_url": download_url,
                    "filename": name,
                    "published_at": published_at,
                }
            )

    snapshots.sort(key=lambda item: item["published_at"])
    return snapshots


def list_archive_snapshots(feed: dict[str, object]) -> list[dict[str, str]]:
    archive_cfg = feed.get("archive")
    if not isinstance(archive_cfg, dict):
        return []

    archive_type = archive_cfg.get("type")
    if archive_type == "mbta_archived_feeds":
        return list_mbta_archive_snapshots(archive_cfg)
    if archive_type == "github_releases":
        return list_github_release_snapshots(archive_cfg)
    if archive_type == "wayback_cdx":
        return list_wayback_snapshots(feed, archive_cfg)

    raise ValueError(f"Unsupported archive type for {feed.get('label')}: {archive_type}")


def list_wayback_snapshots(feed: dict[str, object], archive_cfg: dict[str, object]) -> list[dict[str, str]]:
    source_url = str(archive_cfg.get("source_url") or feed.get("url") or "")
    if not source_url:
        raise ValueError(f"wayback_cdx archive requires a source URL for {feed.get('label')}")

    query = {
        "url": source_url,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": "statuscode:200",
        "limit": str(int(archive_cfg.get("query_limit", 1000))),
        "collapse": str(archive_cfg.get("collapse", "timestamp:8")),
    }
    cdx_url = "https://web.archive.org/cdx/search/cdx?" + urllib.parse.urlencode(query)
    try:
        rows = json.loads(fetch_text(cdx_url))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        return []
    if not isinstance(rows, list) or not rows:
        return []

    snapshots: list[dict[str, str]] = []
    for row in rows[1:]:
        if not isinstance(row, list) or len(row) < 2:
            continue
        timestamp = str(row[0])
        original = str(row[1])
        snapshots.append(
            {
                "download_url": f"https://web.archive.org/web/{timestamp}if_/{original}",
                "filename": basename_from_url(original),
                "service_date": timestamp[:8],
            }
        )

    snapshots.sort(key=lambda item: item["service_date"])
    return snapshots


def filter_archive_snapshots(
    snapshots: list[dict[str, str]],
    archive_since: dt.date | None,
    archive_limit: int,
) -> list[dict[str, str]]:
    filtered = snapshots
    if archive_since is not None:
        filtered = [
            snapshot
            for snapshot in filtered
            if (parse_date_candidate(snapshot.get("service_date") or snapshot.get("published_at")) or dt.date.min)
            >= archive_since
        ]
    if archive_limit > 0:
        filtered = filtered[-archive_limit:]
    return filtered


def download_archive_feed(
    label: str,
    feed: dict[str, object],
    raw_dir: Path,
    force: bool,
    archive_since: dt.date | None,
    archive_limit: int,
    skip_extract: bool,
) -> None:
    archive_dir = raw_dir / label
    if force and archive_dir.exists():
        shutil.rmtree(archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)

    snapshots = filter_archive_snapshots(
        list_archive_snapshots(feed),
        archive_since=archive_since,
        archive_limit=archive_limit,
    )

    if not snapshots:
        if "url" in feed:
            print(f"No archive snapshots selected for {label}; falling back to current feed")
            download_single_feed(
                label,
                str(feed["url"]),
                raw_dir,
                force=force,
                skip_extract=skip_extract,
            )
            return
        print(f"No archive snapshots selected for {label}")
        return

    for index, snapshot in enumerate(snapshots, start=1):
        destination = archive_dir / archive_dest_name(snapshot, index)
        if force or not destination.exists():
            print(f"Downloading archived snapshot for {label}: {snapshot['download_url']}")
            if not safe_download_file(snapshot["download_url"], destination):
                print(f"Skipping archived snapshot for {label}; download failed")
        else:
            print(f"Skipping archived snapshot for {label}; {destination} already exists")


def download_single_feed(
    label: str,
    url: str,
    raw_dir: Path,
    force: bool,
    skip_extract: bool,
) -> None:
    zip_path = raw_dir / f"{label}.zip"
    extract_dir = raw_dir / label

    if force or not zip_path.exists():
        print(f"Downloading {label} from {url}")
        download_file(url, zip_path)
    else:
        print(f"Skipping download for {label}; {zip_path} already exists")

    if not skip_extract:
        if force and extract_dir.exists():
            shutil.rmtree(extract_dir)
        if not extract_dir.exists() or not any(extract_dir.iterdir()):
            print(f"Extracting {zip_path} -> {extract_dir}")
            extract_zip(zip_path, extract_dir)
        else:
            print(f"Skipping extraction for {label}; {extract_dir} is already populated")


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    raw_dir = Path(args.raw_dir)
    feeds = load_config(config_path)
    archive_since = parse_archive_since(args.archive_since)

    if not feeds:
        print(f"No feeds declared in {config_path}", file=sys.stderr)
        return 1

    for feed in feeds:
        label = feed["label"]
        archive_cfg = feed.get("archive")

        if archive_cfg is not None:
            download_archive_feed(
                label,
                feed,
                raw_dir,
                force=args.force,
                archive_since=archive_since,
                archive_limit=args.archive_limit,
                skip_extract=args.skip_extract,
            )
            continue

        download_single_feed(
            label,
            str(feed["url"]),
            raw_dir,
            force=args.force,
            skip_extract=args.skip_extract,
        )

    print(f"GTFS download complete in {raw_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
