# aipass data api exporter

## Overview

`main.py` fetches reservations, sales, and room data from the aipass API and saves them to CSV or JSON files.
The date ranges are split into `history` and `onhand`, and the output destination (local folder and/or S3) is configured in `config.yaml`.

## Setup

1. Create `.env` with your connection info.
2. Adjust `config.yaml` to match your API query parameters, required fields, and output preferences.

Example `.env`:

```
API_BASE_URL=https://api.aipass.jp/public
API_TOKEN=your_token_here
API_TOKEN_HEADER=Authorization
API_ID=your_account_id_here
API_PASSWORD=your_password_here
```

## Run

```
python main.py
```

## Debug fetch (reservations / guests full records)

指定期間で `reservations` と `guests` の全量を切り分け確認したい場合は `debug_fetch.py` を使ってください。

```bash
python debug_fetch.py --start-date 2026-02-01 --end-date 2026-02-28 --output-format json
```

- `--output-format`: `json` or `csv`
- `--output-dir`: 出力先（デフォルト `debug-output`）
- `--csv-encoding`: CSV出力時のエンコード（デフォルト `utf-8-sig`）

このスクリプトは以下の条件で取得します。
- `reservations`: `check_in_date_from` / `check_in_date_to`（`include_related_guest=1`）
- `guests`: `updated_at_from` / `updated_at_to`
- `.env` を自動読込するため、`API_TOKEN` または `API_ID`/`API_PASSWORD` は `.env` に記載すれば利用されます。

## Notes

- The default date offsets are `history: -2 ~ -2` and `onhand: -1 ~ +178` from today.
- To override dates manually, set `date_ranges.manual.enabled: true` and fill `date_ranges.manual.history/onhand.start,end` in `config.yaml`.
- When enabling S3 uploads, ensure AWS credentials are available via environment variables or AWS config files.
- This script is production-only. If `API_BASE_URL` is unset, it defaults to `https://api.aipass.jp/public`.
- If you only have an ID/password (no API token), set `API_ID` and `API_PASSWORD` and leave `API_TOKEN` empty (or keep the placeholder `your_token_here`). The script will request an access token from `{API_BASE_URL}/oauth/token` by default.
- To override the token endpoint, set `API_AUTH_URL` explicitly (e.g., `https://api.aipass.jp/public/oauth/token`).
- エンドポイントごとに仕様上の主要日付項目を使う設定です：`/reservations` は `check_in_date_from/to`、`/sales-details` は `sales_date_from/to`。
- `*_at_from/to` は date-time 形式が必須のため、実行時に `YYYY-MM-DDT00:00:00+09:00` / `YYYY-MM-DDT23:59:59+09:00` に自動変換して送信します。
- `reservations.csv` の顧客項目は `/reservations` レスポンス内の `related_guest` から展開して出力します。
- `sales.csv` はネスト項目（`reservation.reservation_id`、`sales_department.sales_department_name`、`sales_subject.sales_subject_name`）を展開して出力します。
- `rooms.csv` は `/housekeeping` ではなく `/reservations` の `assign_rooms` を展開して出力します。
- CSV 出力は `output.csv.prefix` でファイル名プレフィックス、`output.csv.encoding` で文字コードを設定できます。
- 出力ファイル名の末尾には実行日 `_yyyymmdd` が自動付与されます（例: `reservations_history_20260213.csv`）。
