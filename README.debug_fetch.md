# debug_fetch.py

## Overview

`debug_fetch.py` は指定期間の `reservations` / `guests` / `sales-details` の全レコードを取得し、CSV または JSON として出力するデバッグ用スクリプトです。

## Run

```bash
python debug_fetch.py --start-date 2026-02-01 --end-date 2026-02-28 --output-format json
```

### Options

- `--start-date`: 取得開始日（`YYYY-MM-DD`）
- `--end-date`: 取得終了日（`YYYY-MM-DD`）
- `--output-format`: `json` または `csv`（デフォルト: `json`）
- `--output-dir`: 出力先ディレクトリ（デフォルト: `debug-output`）
- `--csv-encoding`: CSV出力時のエンコード（デフォルト: `utf-8-sig`）

## Query conditions

このスクリプトは以下の条件で取得します。

- `reservations`: `check_in_date_from` / `check_in_date_to`（`include_related_guest=1`）
- `guests`: `updated_at_from` / `updated_at_to`（`YYYY-MM-DDT00:00:00+09:00` / `YYYY-MM-DDT23:59:59+09:00`）
- `sales-details`: `sales_date_from` / `sales_date_to`

## Authentication

`.env` は自動読込されます。以下のいずれかを設定してください。

- `API_TOKEN`
- `API_ID` と `API_PASSWORD`（トークン自動取得）

必要に応じて以下も設定できます。

- `API_BASE_URL`（未設定時: `https://api.aipass.jp/public`）
- `API_TOKEN_HEADER`（未設定時: `Authorization`）
- `API_AUTH_URL`（トークン取得URLを明示する場合）
