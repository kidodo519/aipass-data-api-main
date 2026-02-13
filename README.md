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

## Notes

- The default date offsets are `history: -2 ~ -2` and `onhand: -1 ~ +178` from today.
- To override dates, set the `date_ranges.manual` values in `config.yaml`.
- When enabling S3 uploads, ensure AWS credentials are available via environment variables or AWS config files.
- This script is production-only. If `API_BASE_URL` is unset, it defaults to `https://api.aipass.jp/public`.
- If you only have an ID/password (no API token), set `API_ID` and `API_PASSWORD` and leave `API_TOKEN` empty (or keep the placeholder `your_token_here`). The script will request an access token from `{API_BASE_URL}/oauth/token` by default.
- To override the token endpoint, set `API_AUTH_URL` explicitly (e.g., `https://api.aipass.jp/public/oauth/token`).
- エンドポイントごとに仕様上の主要日付項目を使う設定です：`/reservations` は `check_in_date_from/to`、`/guests` は `created_at_from/to`、`/sales-details` は `sales_date_from/to`、`/housekeeping` は `room_usage_date`。
- `*_at_from/to` は date-time 形式が必須のため、実行時に `YYYY-MM-DDT00:00:00+09:00` / `YYYY-MM-DDT23:59:59+09:00` に自動変換して送信します。
