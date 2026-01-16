import csv
import datetime as dt
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import yaml
except ImportError as exc:
    raise SystemExit("PyYAML is required. Install with `pip install pyyaml`.") from exc

try:
    import requests
except ImportError as exc:
    raise SystemExit("requests is required. Install with `pip install requests`.") from exc

ENV_PATH = Path(".env")
CONFIG_PATH = Path("config.yaml")


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\""))


def load_config(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Config not found: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def resolve_date_range(config: Dict[str, Any], range_name: str) -> Tuple[dt.date, dt.date]:
    manual = config.get("date_ranges", {}).get("manual", {}).get(range_name, {})
    if manual.get("start") and manual.get("end"):
        start = dt.date.fromisoformat(manual["start"])
        end = dt.date.fromisoformat(manual["end"])
        return start, end

    offsets = config.get("date_ranges", {}).get(range_name, {})
    start_offset = int(offsets.get("start_offset_days", 0))
    end_offset = int(offsets.get("end_offset_days", 0))
    today = dt.date.today()
    return today + dt.timedelta(days=start_offset), today + dt.timedelta(days=end_offset)


def parse_link_header(link_header: Optional[str]) -> Dict[str, str]:
    if not link_header:
        return {}
    links: Dict[str, str] = {}
    for part in link_header.split(","):
        if ";" not in part:
            continue
        url_part, rel_part = part.split(";", 1)
        url = url_part.strip().strip("<>")
        rel = rel_part.strip()
        if rel.startswith("rel="):
            rel_value = rel.split("=", 1)[1].strip("\"")
            links[rel_value] = url
    return links


def extract_records(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "results"):
            if isinstance(payload.get(key), list):
                return [item for item in payload[key] if isinstance(item, dict)]
        return [payload]
    return []


def fetch_paginated(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    next_url = url
    next_params = params
    while next_url:
        response = requests.get(next_url, headers=headers, params=next_params, timeout=30)
        response.raise_for_status()
        records.extend(extract_records(response.json()))
        links = parse_link_header(response.headers.get("Link"))
        next_url = links.get("next")
        next_params = None
    return records


def filter_fields(records: Iterable[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    filtered = []
    for record in records:
        filtered.append({field: record.get(field) for field in fields})
    return filtered


def merge_records(
    primary: List[Dict[str, Any]],
    secondary: List[Dict[str, Any]],
    merge_key: str,
) -> List[Dict[str, Any]]:
    if not secondary:
        return primary
    lookup: Dict[Any, List[Dict[str, Any]]] = {}
    for item in secondary:
        key = item.get(merge_key)
        if key is None:
            continue
        lookup.setdefault(key, []).append(item)

    merged: List[Dict[str, Any]] = []
    for base in primary:
        key = base.get(merge_key)
        extras = lookup.get(key)
        if not extras:
            merged.append(base)
            continue
        for extra in extras:
            combined = {**base, **extra}
            merged.append(combined)
    return merged


def write_csv(path: Path, records: List[Dict[str, Any]], fields: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for record in records:
            writer.writerow({field: record.get(field) for field in fields})


def write_json(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def upload_to_s3(path: Path, bucket: str, key: str, region: Optional[str]) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit("boto3 is required for S3 upload. Install with `pip install boto3`.") from exc

    session = boto3.session.Session(region_name=region)
    client = session.client("s3")
    client.upload_file(str(path), bucket, key)


def build_headers(token: str, token_header: str) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if token:
        if token_header.lower() == "authorization" and not token.lower().startswith("bearer "):
            headers[token_header] = f"Bearer {token}"
        else:
            headers[token_header] = token
    return headers


def main() -> None:
    load_env(ENV_PATH)
    config = load_config(CONFIG_PATH)

    base_url = os.environ.get("API_BASE_URL", "").rstrip("/")
    token = os.environ.get("API_TOKEN", "")
    token_header = os.environ.get("API_TOKEN_HEADER", "Authorization")
    if not base_url:
        raise SystemExit("API_BASE_URL is required in .env")

    headers = build_headers(token, token_header)

    output_format = config.get("output", {}).get("format", "csv").lower()
    local_output = config.get("output", {}).get("local_output", {})
    local_enabled = bool(local_output.get("enabled", True))
    local_dir = Path(local_output.get("directory", "processed-csv"))

    s3_config = config.get("output", {}).get("s3", {})
    s3_enabled = bool(s3_config.get("enabled", False))

    ranges = {
        "history": resolve_date_range(config, "history"),
        "onhand": resolve_date_range(config, "onhand"),
    }

    datasets = config.get("datasets", {})
    if not datasets:
        raise SystemExit("No datasets configured in config.yaml")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        for dataset_name, dataset_config in datasets.items():
            sources = dataset_config.get("sources", {})
            if not sources:
                continue
            primary_source_name = dataset_config.get("primary_source") or next(iter(sources.keys()))
            output_fields = dataset_config.get("output_fields", [])

            for range_name, (start_date, end_date) in ranges.items():
                fetched: Dict[str, List[Dict[str, Any]]] = {}
                for source_name, source in sources.items():
                    path = source.get("path", "")
                    date_params = source.get("date_params", {})
                    params = dict(source.get("params", {}))
                    if date_params.get("start"):
                        params[date_params["start"]] = start_date.isoformat()
                    if date_params.get("end"):
                        params[date_params["end"]] = end_date.isoformat()
                    if source.get("per_page"):
                        params["per_page"] = source["per_page"]

                    url = f"{base_url}{path}"
                    records = fetch_paginated(url, headers, params)
                    fields = source.get("fields", [])
                    if fields:
                        records = filter_fields(records, fields)
                    fetched[source_name] = records

                primary_records = fetched.get(primary_source_name, [])
                merge_key = dataset_config.get("merge_key", "reservation_id")

                merged_records = primary_records
                for source_name, records in fetched.items():
                    if source_name == primary_source_name:
                        continue
                    merged_records = merge_records(merged_records, records, merge_key)

                if output_fields:
                    merged_records = filter_fields(merged_records, output_fields)

                extension = "json" if output_format == "json" else "csv"
                filename = f"{dataset_name}_{range_name}.{extension}"
                if local_enabled:
                    output_path = local_dir / filename
                else:
                    output_path = temp_path / filename

                if output_format == "json":
                    write_json(output_path, merged_records)
                else:
                    write_csv(output_path, merged_records, output_fields or sorted(merged_records[0].keys()) if merged_records else [])

                if s3_enabled:
                    bucket = s3_config.get("bucket")
                    prefix = s3_config.get("prefix", "")
                    region = s3_config.get("region")
                    if not bucket:
                        raise SystemExit("S3 bucket is required when s3.enabled is true")
                    key = "/".join(part.strip("/") for part in [prefix, filename] if part)
                    upload_to_s3(output_path, bucket, key, region)

    print("Data export completed.")


if __name__ == "__main__":
    main()
