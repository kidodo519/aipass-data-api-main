import csv
import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from decimal import Decimal, InvalidOperation
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
MAX_REQUEST_RANGE_DAYS = 30


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
    manual_config = config.get("date_ranges", {}).get("manual", {})
    manual_enabled = bool(manual_config.get("enabled", False))
    manual = manual_config.get(range_name, {})
    if manual_enabled and manual.get("start") and manual.get("end"):
        start = dt.date.fromisoformat(manual["start"])
        end = dt.date.fromisoformat(manual["end"])
        return start, end

    offsets = config.get("date_ranges", {}).get(range_name, {})
    start_offset = int(offsets.get("start_offset_days", 0))
    end_offset = int(offsets.get("end_offset_days", 0))
    today = dt.date.today()
    return today + dt.timedelta(days=start_offset), today + dt.timedelta(days=end_offset)


def format_date_param(param_name: str, value: dt.date, is_end: bool) -> str:
    if param_name.endswith("_at_from"):
        return f"{value.isoformat()}T00:00:00+09:00"
    if param_name.endswith("_at_to"):
        return f"{value.isoformat()}T23:59:59+09:00"
    if param_name.endswith("_at"):
        time_part = "23:59:59" if is_end else "00:00:00"
        return f"{value.isoformat()}T{time_part}+09:00"
    return value.isoformat()


def split_date_range(start_date: dt.date, end_date: dt.date, chunk_days: int = MAX_REQUEST_RANGE_DAYS) -> List[Tuple[dt.date, dt.date]]:
    if start_date > end_date:
        raise SystemExit(f"Invalid date range: start={start_date} end={end_date}")

    ranges: List[Tuple[dt.date, dt.date]] = []
    cursor = start_date
    step = dt.timedelta(days=chunk_days - 1)
    while cursor <= end_date:
        chunk_end = min(cursor + step, end_date)
        ranges.append((cursor, chunk_end))
        cursor = chunk_end + dt.timedelta(days=1)
    return ranges


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


def get_nested_value(data: Any, path: str, root: Optional[Dict[str, Any]] = None) -> Any:
    current = root if path.startswith("root.") and root is not None else data
    parts = path.split(".")
    if path.startswith("root."):
        parts = parts[1:]
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def apply_field_paths(
    records: List[Dict[str, Any]],
    field_paths: Dict[str, str],
    root_records: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    if not field_paths:
        return records
    mapped: List[Dict[str, Any]] = []
    for index, record in enumerate(records):
        root_record = root_records[index] if root_records and index < len(root_records) else None
        converted = dict(record)
        for output_field, path_expr in field_paths.items():
            value = None
            for candidate in path_expr.split("|"):
                candidate = candidate.strip()
                if not candidate:
                    continue
                value = get_nested_value(record, candidate, root_record)
                if value is not None:
                    break
            converted[output_field] = value
        mapped.append(converted)
    return mapped


def explode_records(records: List[Dict[str, Any]], explode_key: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    exploded: List[Dict[str, Any]] = []
    roots: List[Dict[str, Any]] = []
    for record in records:
        items = record.get(explode_key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                exploded.append(item)
                roots.append(record)
    return exploded, roots


def fetch_paginated(
    url: str,
    headers: Dict[str, str],
    params: Dict[str, Any],
) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    next_url = url
    next_params = params
    while next_url:
        response = requests.get(next_url, headers=headers, params=next_params, timeout=30)
        if response.status_code >= 400:
            detail = response.text.strip() or "(empty response body)"
            raise SystemExit(
                f"API request failed: {response.status_code} {response.reason} | URL: {response.url} | body: {detail}"
            )
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


def add_constant_field(records: List[Dict[str, Any]], field_name: str, value: Any) -> List[Dict[str, Any]]:
    return [{**record, field_name: value} for record in records]


def parse_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def normalize_decimal_output(value: Decimal) -> Any:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def apply_computed_fields(records: List[Dict[str, Any]], computed_fields: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not computed_fields:
        return records

    computed_records: List[Dict[str, Any]] = []
    for record in records:
        updated = dict(record)
        for output_field, definition in computed_fields.items():
            operation = str(definition.get("operation", "")).strip().lower()
            if operation == "subtract":
                left = parse_decimal(updated.get(definition.get("left")))
                right = parse_decimal(updated.get(definition.get("right")))
                if left is None or right is None:
                    updated[output_field] = None
                else:
                    updated[output_field] = normalize_decimal_output(left - right)
            elif operation == "concat":
                fields = definition.get("fields", [])
                separator = str(definition.get("separator", " "))
                values = [str(updated.get(field) or "").strip() for field in fields]
                updated[output_field] = separator.join(value for value in values if value)
            else:
                updated[output_field] = None
        computed_records.append(updated)
    return computed_records


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


def write_csv(path: Path, records: List[Dict[str, Any]], fields: List[str], encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=encoding, newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields)
        writer.writeheader()
        for record in records:
            writer.writerow({field: record.get(field) for field in fields})


def write_json(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def upload_to_s3(
    path: Path,
    bucket_name: str,
    file_name: str,
    access_key_id: str,
    secret_access_key: str,
    region: Optional[str],
) -> None:
    try:
        import boto3
    except ImportError as exc:
        raise SystemExit("boto3 is required for S3 upload. Install with `pip install boto3`.") from exc

    session = boto3.session.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region,
    )
    client = session.client("s3")
    client.upload_file(str(path), bucket_name, file_name)


def resolve_s3_file_name(configured_name: str, generated_file_name: str) -> str:
    configured = configured_name.strip()
    if not configured:
        return generated_file_name

    if "{file_name}" in configured:
        return configured.replace("{file_name}", generated_file_name)

    return f"{configured.rstrip('/')}/{generated_file_name}"


def build_headers(token: str, token_header: str) -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if token:
        if token_header.lower() == "authorization" and not token.lower().startswith("bearer "):
            headers[token_header] = f"Bearer {token}"
        else:
            headers[token_header] = token
    return headers


def fetch_access_token(auth_url: str, user_id: str, password: str) -> str:
    response = requests.post(
        auth_url,
        data={"email": user_id, "password": password},
        timeout=30,
    )
    if response.status_code >= 400:
        detail = response.text.strip() or "(empty response body)"
        raise SystemExit(
            f"Token request failed: {response.status_code} {response.reason} | URL: {response.url} | body: {detail}"
        )
    payload = response.json()
    token = payload.get("access_token")
    if not token:
        raise SystemExit("access_token not found in auth response.")
    return str(token)


def main() -> None:
    load_env(ENV_PATH)
    config = load_config(CONFIG_PATH)

    api_env = os.environ.get("API_ENV", "").strip().lower()
    base_url = os.environ.get("API_BASE_URL", "").rstrip("/")
    if not base_url:
        base_url = "https://api.aipass.jp/public"
    token = os.environ.get("API_TOKEN", "")
    token_header = os.environ.get("API_TOKEN_HEADER", "Authorization")
    user_id = os.environ.get("API_ID", "")
    password = os.environ.get("API_PASSWORD", "")
    auth_url = os.environ.get("API_AUTH_URL", f"{base_url}/oauth/token")
    if token.strip().lower() in {"", "your_token_here"}:
        token = ""

    if not token:
        if not (user_id and password):
            raise SystemExit("API_ID and API_PASSWORD are required to fetch an access token.")
        token = fetch_access_token(auth_url, user_id, password)

    headers = build_headers(token, token_header)

    output_format = config.get("output", {}).get("format", "csv").lower()
    csv_config = config.get("output", {}).get("csv", {})
    csv_prefix = str(csv_config.get("prefix", "")).strip()
    csv_encoding = str(csv_config.get("encoding", "utf-8-sig")).strip() or "utf-8-sig"
    output_date_suffix = dt.date.today().strftime("%Y%m%d")
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
                    url = f"{base_url}{path}"
                    date_chunks = split_date_range(start_date, end_date)
                    all_records: List[Dict[str, Any]] = []
                    for chunk_start, chunk_end in date_chunks:
                        params = dict(source.get("params", {}))
                        if date_params.get("start"):
                            params[date_params["start"]] = format_date_param(date_params["start"], chunk_start, is_end=False)
                        if date_params.get("end"):
                            params[date_params["end"]] = format_date_param(date_params["end"], chunk_end, is_end=True)
                        if source.get("per_page"):
                            params["per_page"] = source["per_page"]

                        records = fetch_paginated(url, headers, params)
                        explode_key = source.get("explode")
                        root_records = None
                        if explode_key:
                            records, root_records = explode_records(records, explode_key)

                        field_paths = source.get("field_paths", {})
                        if field_paths:
                            records = apply_field_paths(records, field_paths, root_records)

                        fields = source.get("fields", [])
                        if fields:
                            records = filter_fields(records, fields)
                        all_records.extend(records)

                    fetched[source_name] = all_records

                primary_records = fetched.get(primary_source_name, [])
                merge_key = dataset_config.get("merge_key", "reservation_id")

                merged_records = primary_records
                for source_name, records in fetched.items():
                    if source_name == primary_source_name:
                        continue
                    merged_records = merge_records(merged_records, records, merge_key)

                computed_fields = dataset_config.get("computed_fields", {})
                if computed_fields:
                    merged_records = apply_computed_fields(merged_records, computed_fields)

                if output_fields:
                    merged_records = filter_fields(merged_records, output_fields)

                if output_format == "csv":
                    merged_records = add_constant_field(merged_records, "facility_id", 1)
                    if output_fields and "facility_id" not in output_fields:
                        output_fields = [*output_fields, "facility_id"]

                extension = "json" if output_format == "json" else "csv"
                base_name = f"{dataset_name}_{range_name}_{output_date_suffix}"
                if output_format == "csv" and csv_prefix:
                    base_name = f"{csv_prefix}{base_name}"
                filename = f"{base_name}.{extension}"
                if local_enabled:
                    output_path = local_dir / filename
                else:
                    output_path = temp_path / filename

                if output_format == "json":
                    write_json(output_path, merged_records)
                else:
                    write_csv(
                        output_path,
                        merged_records,
                        output_fields or sorted(merged_records[0].keys()) if merged_records else [],
                        csv_encoding,
                    )

                if s3_enabled:
                    bucket_name = str(s3_config.get("bucket_name", "")).strip()
                    file_name = resolve_s3_file_name(str(s3_config.get("file_name", "")), filename)
                    access_key_id = str(s3_config.get("access_key_id", "")).strip()
                    secret_access_key = str(s3_config.get("secret_access_key", "")).strip()
                    region = s3_config.get("region")

                    if not bucket_name:
                        raise SystemExit("s3.bucket_name is required when s3.enabled is true")
                    if not access_key_id:
                        raise SystemExit("s3.access_key_id is required when s3.enabled is true")
                    if not secret_access_key:
                        raise SystemExit("s3.secret_access_key is required when s3.enabled is true")

                    upload_to_s3(
                        output_path,
                        bucket_name,
                        file_name,
                        access_key_id,
                        secret_access_key,
                        region,
                    )

    print("Data export completed.")


if __name__ == "__main__":
    main()
