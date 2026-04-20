#!/usr/bin/env python3
"""
Download GTFS feeds declared in config/feeds.json.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="final_project/config/feeds.json",
        help="Path to the feed configuration JSON",
    )
    parser.add_argument(
        "--raw-dir",
        default="final_project/data/raw",
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


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as archive:
        archive.extractall(extract_dir)


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    raw_dir = Path(args.raw_dir)
    feeds = load_config(config_path)

    if not feeds:
        print(f"No feeds declared in {config_path}", file=sys.stderr)
        return 1

    for feed in feeds:
        label = feed["label"]
        url = feed["url"]
        zip_path = raw_dir / f"{label}.zip"
        extract_dir = raw_dir / label

        if args.force or not zip_path.exists():
            print(f"Downloading {label} from {url}")
            download_file(url, zip_path)
        else:
            print(f"Skipping download for {label}; {zip_path} already exists")

        if not args.skip_extract:
            if args.force and extract_dir.exists():
                shutil.rmtree(extract_dir)
            if not extract_dir.exists() or not any(extract_dir.iterdir()):
                print(f"Extracting {zip_path} -> {extract_dir}")
                extract_zip(zip_path, extract_dir)
            else:
                print(f"Skipping extraction for {label}; {extract_dir} is already populated")

    print(f"GTFS download complete in {raw_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
