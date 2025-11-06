
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
json_to_csv.py
----------------
Reads all .json and .jsonl files in an input directory, validates each record against
a fixed schema, extracts required fields (excluding 'questions'), and writes a single CSV.

Special handling:
- For `themes`, captures ONLY the `emotion` of the LAST theme object (if present).
- Skips files/records that fail schema verification (with clear logs).
- Creates the output directory if missing: /output/tableStructureed
- Output filename defaults to `calls.csv` (can be overridden with --output-filename).

Usage:
    python json_to_csv.py /path/to/folder --output-filename calls.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

JsonLike = Dict[str, Any]

# ------------------------------
# Configuration
# ------------------------------
# Final CSV schema (column order):
CSV_SCHEMA: List[Tuple[str, str]] = [
    ("callid", "str"),
    ("filename", "str"),
    ("timestamp", "str"),
    ("agent", "str"),
    ("account_id", "str"),
    ("total_call_time", "float"),
    ("primary_reason", "str"),
    ("call_type", "str"),
    ("call_category", "str"),
    ("call_outcome", "str"),
    ("last_theme_emotion", "str"),  # derived from themes[-1]['emotion']
    ("sentiment_score", "int"),
    ("food_program", "bool"),
]

REQUIRED_FIELDS = {
    # required top-level fields; the 'themes' list is optional but used to derive last_theme_emotion
    "callid", "filename", "timestamp", "agent", "account_id", "total_call_time",
    "primary_reason", "call_type", "call_category", "call_outcome", "sentiment_score", "food_program",
}

# ------------------------------
# Utilities
# ------------------------------
def coerce(value: Any, typ: str) -> Any:
    if value is None:
        return None
    try:
        if typ == "str":
            return str(value)
        if typ == "int":
            if isinstance(value, bool):
                return int(value)
            return int(float(value))  # handle "3.0" or 3.0 -> 3
        if typ == "float":
            return float(value)
        if typ == "bool":
            if isinstance(value, bool):
                return value
            s = str(value).strip().lower()
            if s in {"true", "1", "yes", "y"}:
                return True
            if s in {"false", "0", "no", "n"}:
                return False
            raise ValueError(f"Cannot coerce to bool: {value!r}")
    except Exception as e:
        raise ValueError(f"Failed coercion {value!r} -> {typ}: {e}") from e
    raise ValueError(f"Unknown type: {typ}")


def validate_schema(record: JsonLike) -> Tuple[bool, List[str]]:
    """Return (is_valid, errors). Verifies presence & basic type-compatibility before extraction."""
    errors: List[str] = []
    missing = REQUIRED_FIELDS - set(record.keys())
    if missing:
        errors.append(f"Missing required fields: {sorted(missing)}")

    # lightweight type checks (best-effort; strict coercion happens later)
    numeric_like = (int, float)
    if "total_call_time" in record and not isinstance(record["total_call_time"], (*numeric_like, str, bool)):
        errors.append("total_call_time must be numeric-like or string")
    if "sentiment_score" in record and not isinstance(record["sentiment_score"], (*numeric_like, str, bool)):
        errors.append("sentiment_score must be numeric-like or string")
    if "food_program" in record and not isinstance(record["food_program"], (bool, str, int)):
        errors.append("food_program must be boolean-like")

    return (len(errors) == 0, errors)


def extract_row(record: JsonLike) -> List[Any]:
    """Extract fields per CSV_SCHEMA, applying coercions and derived logic."""
    # derive last_theme_emotion
    last_theme_emotion: Optional[str] = None
    try:
        themes = record.get("themes")
        if isinstance(themes, list) and themes:
            last = themes[-1]
            if isinstance(last, dict):
                emo = last.get("emotion")
                if emo is not None:
                    last_theme_emotion = str(emo)
    except Exception:
        last_theme_emotion = None

    # build row by schema order
    values: List[Any] = []
    for col, typ in CSV_SCHEMA:
        if col == "last_theme_emotion":
            values.append(last_theme_emotion)
            continue
        raw = record.get(col)
        coerced = coerce(raw, typ) if raw is not None else None
        values.append(coerced)
    return values


def iter_input_files(root: Path) -> Iterable[Path]:
    for p in sorted(root.rglob("*")):
        if p.is_file() and p.suffix.lower() in {".json", ".jsonl"}:
            yield p


def read_json_records(path: Path) -> Iterable[JsonLike]:
    suffix = path.suffix.lower()
    try:
        if suffix == ".jsonl":
            with path.open("r", encoding="utf-8") as f:
                for lineno, line in enumerate(f, start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        if isinstance(obj, dict):
                            yield obj
                        else:
                            logging.warning("%s line %d: expected JSON object, got %s", path, lineno, type(obj).__name__)
                    except json.JSONDecodeError as e:
                        logging.error("%s line %d: invalid JSON: %s", path, lineno, e)
        else:  # .json
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                yield data
            elif isinstance(data, list):
                for idx, obj in enumerate(data, start=1):
                    if isinstance(obj, dict):
                        yield obj
                    else:
                        logging.warning("%s idx %d: expected JSON object in list, got %s", path, idx, type(obj).__name__)
            else:
                logging.warning("%s: top-level JSON is not object or array; skipping", path)
    except Exception as e:
        logging.exception("Failed reading %s: %s", path, e)


def write_csv(rows: Iterable[List[Any]], output_dir: Path, output_filename: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / output_filename
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([c for c, _ in CSV_SCHEMA])
        for row in rows:
            writer.writerow(row)
    return out_path


def process_folder(input_dir: Union[str, Path], output_dir: Union[str, Path] = "/output/tableStructureed", output_filename: str = "calls.csv") -> Path:
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)

    if not input_dir.exists() or not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found or not a directory: {input_dir}")

    logging.info("Scanning for JSON/JSONL in %s", input_dir)
    good_rows: List[List[Any]] = []
    total_records = 0
    skipped = 0

    for file_path in iter_input_files(input_dir):
        logging.info("Reading %s", file_path)
        for rec in read_json_records(file_path):
            total_records += 1
            ok, errs = validate_schema(rec)
            if not ok:
                skipped += 1
                logging.warning("Schema validation failed for record in %s: %s", file_path, "; ".join(errs))
                continue
            try:
                row = extract_row(rec)
                good_rows.append(row)
            except Exception as e:
                skipped += 1
                logging.exception("Failed to extract row from %s: %s", file_path, e)

    out_path = write_csv(good_rows, output_dir, output_filename)
    logging.info("Wrote %d rows (skipped %d of %d) to %s", len(good_rows), skipped, total_records, out_path)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract fields from JSON/JSONL to CSV (production-grade)." )
    parser.add_argument("input_dir", help="Directory containing .json/.jsonl files (recursively scanned)")
    parser.add_argument("--output-filename", default="calls.csv", help="CSV filename to write inside the output directory")
    parser.add_argument("--output-dir", default="/output/tableStructureed", help="Output directory to write the CSV to (will be created)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG","INFO","WARNING","ERROR","CRITICAL"], help="Logging level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    out = process_folder(args.input_dir, args.output_dir, args.output_filename)
    print(str(out))


if __name__ == "__main__":
    main()
