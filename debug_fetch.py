import argparse
import csv
import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


DEFAULT_BASE_URL = "https://api.aipass.jp/public"


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
        os.environ.setdefault(key.strip(), value.strip().strip('"'))


def parse_date_arg(raw_value: str, arg_name: str) -> dt.date:
    try:
        return dt.date.fromisoformat(raw_value)
    except ValueError as exc:
        raise SystemExit(
            f"Invalid {arg_name}: '{raw_value}'. Use YYYY-MM-DD with a real calendar date (e.g. 2026-02-28)."
        ) from exc


def build_headers(token: str, token_header: str = "Authorization") -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if token:
        if token_header.lower() == "authorization" and not token.lower().startswith("bearer "):
            headers[token_header] = f"Bearer {token}"
        else:
            headers[token_header] = token
    return headers


def fetch_access_token(base_url: str, user_id: str, password: str, auth_url: Optional[str] = None) -> str:
    token_url = auth_url or f"{base_url.rstrip('/')}/oauth/token"
    response = requests.post(token_url, data={"email": user_id, "password": password}, timeout=30)
    if response.status_code >= 400:
        raise SystemExit(f"Token request failed: {response.status_code} {response.reason} | {response.text}")
    token = response.json().get("access_token")
    if not token:
        raise SystemExit("access_token not found in auth response")
    return str(token)


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
            links[rel.split("=", 1)[1].strip('"')] = url
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
    next_params: Optional[Dict[str, Any]] = params
    while next_url:
        response = requests.get(next_url, headers=headers, params=next_params, timeout=30)
        if response.status_code >= 400:
            raise SystemExit(
                f"API request failed: {response.status_code} {response.reason} | URL: {response.url} | body: {response.text}"
            )
        records.extend(extract_records(response.json()))
        links = parse_link_header(response.headers.get("Link"))
        next_url = links.get("next")
        next_params = None
    return records


def serialize_cell(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def write_csv(path: Path, records: List[Dict[str, Any]], encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers: List[str] = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)

    with path.open("w", encoding=encoding, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for record in records:
            writer.writerow({k: serialize_cell(record.get(k)) for k in headers})


def write_json(path: Path, records: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    load_env(Path(".env"))

    parser = argparse.ArgumentParser(
        description="Debug fetch for reservations/guests/sales-details full data in a period"
    )
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output-format", choices=["csv", "json"], default="json")
    parser.add_argument("--output-dir", default="debug-output")
    parser.add_argument("--csv-encoding", default="utf-8-sig")
    args = parser.parse_args()

    start = parse_date_arg(args.start_date, "--start-date")
    end = parse_date_arg(args.end_date, "--end-date")
    if start > end:
        raise SystemExit(f"Invalid date range: --start-date ({start}) must be <= --end-date ({end}).")

    base_url = os.environ.get("API_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    token = os.environ.get("API_TOKEN", "")
    if token.strip().lower() in {"", "your_token_here"}:
        token = ""

    if not token:
        user_id = os.environ.get("API_ID", "")
        password = os.environ.get("API_PASSWORD", "")
        if not (user_id and password):
            raise SystemExit("Set API_TOKEN or both API_ID/API_PASSWORD")
        token = fetch_access_token(base_url, user_id, password, os.environ.get("API_AUTH_URL"))

    headers = build_headers(token, os.environ.get("API_TOKEN_HEADER", "Authorization"))

    output_date = dt.date.today().strftime("%Y%m%d")
    output_dir = Path(args.output_dir)

    endpoint_specs = {
        "reservations": {
            "path": "/reservations",
            "params": {
                "check_in_date_from": start.isoformat(),
                "check_in_date_to": end.isoformat(),
                "include_related_guest": 1,
                "per_page": 1000,
            },
        },
        "guests": {
            "path": "/guests",
            "params": {
                "updated_at_from": f"{start.isoformat()}T00:00:00+09:00",
                "updated_at_to": f"{end.isoformat()}T23:59:59+09:00",
                "per_page": 1000,
            },
        },
        "sales-details": {
            "path": "/sales-details",
            "params": {
                "sales_date_from": start.isoformat(),
                "sales_date_to": end.isoformat(),
                "per_page": 1000,
            },
        },
    }

    for name, spec in endpoint_specs.items():
        url = f"{base_url}{spec['path']}"
        records = fetch_paginated(url, headers, spec["params"])
        filename = f"debug_{name}_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}_{output_date}.{args.output_format}"
        output_path = output_dir / filename
        if args.output_format == "json":
            write_json(output_path, records)
        else:
            write_csv(output_path, records, args.csv_encoding)
        print(f"{name}: {len(records)} records -> {output_path}")


if __name__ == "__main__":
    main()
