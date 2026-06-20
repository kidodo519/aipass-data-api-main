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

## Logs

実行ごとに `config.yaml` の `logging.directory`（既定: `logs`）配下へ `aipass_export_yyyymmdd_HHMMSS.log` を出力します。
ログには、データセット・期間（history/onhand）・ソース・日付分割チャンクごとのリクエストURL/パラメータ、HTTPステータス、取得件数、`assign_rooms` 展開前後の件数、出力ファイルパス、S3アップロード結果が記録されます。
`rooms_onhand` が空になる場合は、ログ内の `dataset=rooms range=onhand` と `Exploded records` の行を確認してください。`before` が0なら `/reservations` の取得結果自体が0件、`before` があるのに `after` が0ならレスポンス内の `assign_rooms` が空または存在しない可能性があります。

## Notes

- The default date offsets are `history: -2 ~ -2` and `onhand: -1 ~ +178` from today.
- To override dates manually, set `date_ranges.manual.enabled: true` and fill `date_ranges.manual.history/onhand.start,end` in `config.yaml`.
- S3アップロードを有効化する場合は `config.yaml` の `output.s3.bucket_name` / `access_key_id` / `secret_access_key` を設定してください。`output.s3.file_name` は任意で、未設定時はCSV/JSON出力と同名、`aipass/exports` のように指定するとそのプレフィックス配下へ同名保存、`{file_name}` を含めると置換して保存します。
- This script is production-only. If `API_BASE_URL` is unset, it defaults to `https://api.aipass.jp/public`.
- If you only have an ID/password (no API token), set `API_ID` and `API_PASSWORD` and leave `API_TOKEN` empty (or keep the placeholder `your_token_here`). The script will request an access token from `{API_BASE_URL}/oauth/token` by default.
- To override the token endpoint, set `API_AUTH_URL` explicitly (e.g., `https://api.aipass.jp/public/oauth/token`).
- エンドポイントごとに仕様上の主要日付項目を使う設定です：`/reservations` は `check_in_date_from/to`、`/sales-details` は `sales_date_from/to`。
- API仕様制限に合わせ、日付範囲は内部的に分割して順次取得し、合算した結果を最終CSV/JSONとして出力します。既定は30日単位ですが、各 source に `date_chunk_days` を設定すると分割幅を変更できます。大量件数になりやすい `rooms` は `config.yaml` で7日単位に設定しています。
- `*_at_from/to` は date-time 形式が必須のため、実行時に `YYYY-MM-DDT00:00:00+09:00` / `YYYY-MM-DDT23:59:59+09:00` に自動変換して送信します。
- `reservations.csv` の顧客項目は `/reservations` レスポンス内の `related_guest` から展開して出力します。
- `related_guest` を確実に返すため、`main.py` の `/reservations` リクエストでは `include_related_guest=1` を付与しています。
- `sales.csv` はネスト項目（`reservation.reservation_id`、`sales_department.sales_department_name`、`sales_subject.sales_subject_name`）を展開して出力します。
- `rooms.csv` は `/housekeeping` ではなく `/reservations` の `assign_rooms` を展開して出力します。
- CSV 出力は `output.csv.prefix` でファイル名プレフィックス、`output.csv.encoding` で文字コードを設定できます。
- 出力ファイル名の末尾には実行日 `_yyyymmdd` が自動付与されます（例: `reservations_history_20260213.csv`）。

## debug_fetch.py

`debug_fetch.py` の使い方は `README.debug_fetch.md` を参照してください。
