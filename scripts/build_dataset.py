#!/usr/bin/env python3
"""
Build a fixed-width trip-record dataset from multiple GTFS feeds.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import struct
import sys
import zipfile
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

ROOT_DIR = Path(__file__).resolve().parent.parent


TRIP_RECORD = struct.Struct("<12I")
DEFAULT_SERVICE_MASK = 0b1111111


@dataclass(frozen=True)
class TripInfo:
    route_hash: int
    route_type: int
    service_mask: int
    direction_id: int
    trip_hash: int


@dataclass
class TripSummary:
    min_stop_sequence: int
    max_stop_sequence: int
    start_secs: int
    end_secs: int
    stop_count: int


@dataclass(frozen=True)
class GTFSSource:
    name: str
    directory: Path | None = None
    archive: Path | None = None
    archive_member: str | None = None


class GTFSReader:
    def __init__(self, raw_dir: Path, label: str) -> None:
        self.label = label
        self.directory = raw_dir / label
        self.zip_path = raw_dir / f"{label}.zip"
        self.sources = self._discover_sources()

    def _discover_sources(self) -> list[GTFSSource]:
        sources: list[GTFSSource] = []

        if self.directory.exists():
            top_level_txt = sorted(self.directory.glob("*.txt"))
            nested_zips = sorted(self.directory.glob("*.zip"))

            if top_level_txt:
                sources.append(GTFSSource(name=self.label, directory=self.directory))
            elif nested_zips:
                for nested_zip in nested_zips:
                    sources.extend(self._discover_archive_sources(nested_zip, f"{self.label}/{nested_zip.stem}"))

        if not sources and self.zip_path.exists():
            sources.extend(self._discover_archive_sources(self.zip_path, self.label))

        return sources

    def _discover_archive_sources(self, archive_path: Path, source_name: str) -> list[GTFSSource]:
        with zipfile.ZipFile(archive_path) as archive:
            top_level_txt = sorted(
                name for name in archive.namelist() if name.lower().endswith(".txt")
            )
            nested_zips = sorted(
                name for name in archive.namelist() if name.lower().endswith(".zip")
            )

        if top_level_txt:
            return [GTFSSource(name=source_name, archive=archive_path)]

        if nested_zips:
            return [
                GTFSSource(
                    name=f"{source_name}/{Path(nested_zip).stem}",
                    archive=archive_path,
                    archive_member=nested_zip,
                )
                for nested_zip in nested_zips
            ]

        return [GTFSSource(name=source_name, archive=archive_path)]

    def _resolve_directory_member(self, base_dir: Path, filename: str) -> Path | None:
        direct_path = base_dir / filename
        if direct_path.exists():
            return direct_path

        matches = sorted(base_dir.rglob(filename))
        if matches:
            return matches[0]

        return None

    def _resolve_archive_member(self, archive: zipfile.ZipFile, filename: str) -> str | None:
        names = archive.namelist()
        if filename in names:
            return filename

        suffix = f"/{filename}"
        for name in names:
            if name.endswith(suffix) or name == filename:
                return name

        return None

    def _iter_nested_archive_rows(
        self,
        archive: zipfile.ZipFile,
        nested_member: str,
        filename: str,
    ) -> Iterator[dict[str, str]]:
        with archive.open(nested_member, "r") as nested_handle:
            nested_bytes = nested_handle.read()

        with zipfile.ZipFile(io.BytesIO(nested_bytes)) as nested_archive:
            member_name = self._resolve_archive_member(nested_archive, filename)
            if member_name is None:
                return

            with nested_archive.open(member_name, "r") as raw_handle:
                with io.TextIOWrapper(raw_handle, encoding="utf-8-sig", newline="") as handle:
                    for row in csv.DictReader(handle):
                        yield row

    def iter_csv_rows(self, filename: str, required: bool = True) -> Iterator[tuple[str, dict[str, str]]]:
        matched = False

        for source in self.sources:
            if source.directory is not None:
                csv_path = self._resolve_directory_member(source.directory, filename)
                if csv_path is None:
                    continue
                matched = True
                with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
                    for row in csv.DictReader(handle):
                        yield source.name, row
                continue

            if source.archive is not None:
                with zipfile.ZipFile(source.archive) as archive:
                    if source.archive_member is not None:
                        nested_rows = self._iter_nested_archive_rows(
                            archive,
                            source.archive_member,
                            filename,
                        )
                        found_row = False
                        for row in nested_rows:
                            if not found_row:
                                matched = True
                                found_row = True
                            yield source.name, row
                        continue

                    member_name = self._resolve_archive_member(archive, filename)
                    if member_name is None:
                        continue
                    matched = True
                    with archive.open(member_name, "r") as raw_handle:
                        with io.TextIOWrapper(raw_handle, encoding="utf-8-sig", newline="") as handle:
                            for row in csv.DictReader(handle):
                                yield source.name, row

        if required and not matched:
            raise FileNotFoundError(f"{filename} not found for feed {self.label}")


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
        help="Directory containing downloaded GTFS feeds",
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT_DIR / "data" / "processed"),
        help="Directory where the binary dataset and manifest should be written",
    )
    parser.add_argument(
        "--output",
        default="transit_trip_records.bin",
        help="Binary dataset filename inside --out-dir",
    )
    parser.add_argument(
        "--manifest",
        default="transit_trip_records_manifest.json",
        help="Manifest filename inside --out-dir",
    )
    return parser.parse_args()


def load_config(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def parse_gtfs_time(value: str) -> int | None:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        return None
    parts = stripped.split(":")
    if len(parts) != 3:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
    except ValueError:
        return None
    return hours * 3600 + minutes * 60 + seconds


def safe_int(value: str | None, default: int = 0) -> int:
    if value is None:
        return default
    stripped = value.strip()
    if not stripped:
        return default
    try:
        return int(stripped)
    except ValueError:
        return default


def stable_hash(text: str) -> int:
    return zlib.crc32(text.encode("utf-8")) & 0xFFFFFFFF


def namespaced_id(source_name: str, raw_id: str) -> str:
    return f"{source_name}:{raw_id}"


def read_calendar_masks(reader: GTFSReader) -> dict[str, int]:
    masks: dict[str, int] = {}
    bit_columns = [
        ("monday", 0),
        ("tuesday", 1),
        ("wednesday", 2),
        ("thursday", 3),
        ("friday", 4),
        ("saturday", 5),
        ("sunday", 6),
    ]

    try:
        for source_name, row in reader.iter_csv_rows("calendar.txt", required=False):
            mask = 0
            for column, bit in bit_columns:
                if safe_int(row.get(column), 0):
                    mask |= 1 << bit
            masks[namespaced_id(source_name, row["service_id"])] = mask or DEFAULT_SERVICE_MASK
    except FileNotFoundError:
        pass

    return masks


def read_routes(reader: GTFSReader) -> dict[str, int]:
    routes: dict[str, int] = {}
    for source_name, row in reader.iter_csv_rows("routes.txt", required=True):
        routes[namespaced_id(source_name, row["route_id"])] = safe_int(
            row.get("route_type"),
            TRANSIT_ROUTE_TYPE_FALLBACK,
        )
    return routes


TRANSIT_ROUTE_TYPE_FALLBACK = 15


def read_trips(reader: GTFSReader, routes: dict[str, int], calendar_masks: dict[str, int]) -> dict[str, TripInfo]:
    trips: dict[str, TripInfo] = {}
    for source_name, row in reader.iter_csv_rows("trips.txt", required=True):
        route_id = namespaced_id(source_name, row["route_id"])
        trip_id = namespaced_id(source_name, row["trip_id"])
        service_id = namespaced_id(source_name, row.get("service_id", ""))
        direction_id = safe_int(row.get("direction_id"), 0)
        trips[trip_id] = TripInfo(
            route_hash=stable_hash(route_id),
            route_type=routes.get(route_id, TRANSIT_ROUTE_TYPE_FALLBACK),
            service_mask=calendar_masks.get(service_id, DEFAULT_SERVICE_MASK),
            direction_id=direction_id,
            trip_hash=stable_hash(trip_id),
        )
    return trips


def summarize_stop_times(reader: GTFSReader) -> dict[str, TripSummary]:
    summaries: dict[str, TripSummary] = {}
    for source_name, row in reader.iter_csv_rows("stop_times.txt", required=True):
        trip_id = namespaced_id(source_name, row["trip_id"])
        arrival_secs = parse_gtfs_time(row.get("arrival_time", ""))
        departure_secs = parse_gtfs_time(row.get("departure_time", ""))
        sequence = safe_int(row.get("stop_sequence"), 0)
        start_value = departure_secs if departure_secs is not None else arrival_secs
        end_value = arrival_secs if arrival_secs is not None else departure_secs

        if start_value is None or end_value is None:
            continue

        summary = summaries.get(trip_id)
        if summary is None:
            summaries[trip_id] = TripSummary(
                min_stop_sequence=sequence,
                max_stop_sequence=sequence,
                start_secs=start_value,
                end_secs=end_value,
                stop_count=1,
            )
            continue

        summary.stop_count += 1
        if sequence <= summary.min_stop_sequence:
            summary.min_stop_sequence = sequence
            summary.start_secs = start_value
        if sequence >= summary.max_stop_sequence:
            summary.max_stop_sequence = sequence
            summary.end_secs = end_value

    return summaries


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    output_path = out_dir / args.output
    manifest_path = out_dir / args.manifest

    feeds = load_config(config_path)
    if not feeds:
        print(f"No feeds found in {config_path}", file=sys.stderr)
        return 1

    out_dir.mkdir(parents=True, exist_ok=True)

    city_ids: dict[str, int] = {}
    feed_summaries: list[dict[str, object]] = []
    city_counts: dict[str, int] = {}
    records: list[tuple[int, ...]] = []

    for feed_id, feed in enumerate(feeds):
        label = feed["label"]
        city = feed["city"]
        city_id = city_ids.setdefault(city, len(city_ids))
        reader = GTFSReader(raw_dir, label)

        try:
            calendar_masks = read_calendar_masks(reader)
            routes = read_routes(reader)
            trips = read_trips(reader, routes, calendar_masks)
            summaries = summarize_stop_times(reader)
        except FileNotFoundError as exc:
            print(f"Skipping {label}: {exc}", file=sys.stderr)
            continue

        feed_trip_count = 0
        for trip_id, trip_info in trips.items():
            summary = summaries.get(trip_id)
            if summary is None:
                continue

            duration = max(summary.end_secs - summary.start_secs, 0)
            record = (
                city_id,
                feed_id,
                trip_info.route_type & 0xFFFFFFFF,
                trip_info.route_hash,
                trip_info.service_mask & 0xFFFFFFFF,
                trip_info.direction_id & 0xFFFFFFFF,
                summary.start_secs & 0xFFFFFFFF,
                summary.end_secs & 0xFFFFFFFF,
                duration & 0xFFFFFFFF,
                summary.stop_count & 0xFFFFFFFF,
                trip_info.trip_hash,
                0,
            )
            records.append(record)
            feed_trip_count += 1

        city_counts[city] = city_counts.get(city, 0) + feed_trip_count
        feed_summaries.append(
            {
                "feed_id": feed_id,
                "label": label,
                "city": city,
                "city_id": city_id,
                "trip_records": feed_trip_count,
                "routes": len(routes),
                "trips": len(trips),
            }
        )
        print(
            f"Prepared {feed_trip_count} trip records from {label} "
            f"({len(trips)} trips, city={city})"
        )

    if not records:
        print("No records were generated; check your GTFS downloads", file=sys.stderr)
        return 1

    records.sort(key=lambda row: (row[0], row[3], row[5], row[6], row[10]))

    with output_path.open("wb") as handle:
        for record in records:
            handle.write(TRIP_RECORD.pack(*record))

    manifest = {
        "record_size_bytes": TRIP_RECORD.size,
        "record_count": len(records),
        "cities": [{"city_id": city_id, "city": city} for city, city_id in city_ids.items()],
        "city_trip_counts": city_counts,
        "feeds": feed_summaries,
        "binary_path": str(output_path),
        "sort_key": ["city_id", "route_hash", "direction_id", "start_secs", "trip_hash"],
        "schema": [
            "city_id",
            "feed_id",
            "route_type",
            "route_hash",
            "service_mask",
            "direction_id",
            "start_secs",
            "end_secs",
            "duration_secs",
            "stop_count",
            "trip_hash",
            "reserved",
        ],
    }

    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print(f"Wrote {len(records)} records to {output_path}")
    print(f"Wrote manifest to {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
