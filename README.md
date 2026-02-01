# aipass data api exporter

## Overview

`main.py` fetches reservations, sales, and room data from the aipass API and saves them to CSV or JSON files.
The date ranges are split into `history` and `onhand`, and the output destination (local folder and/or S3) is configured in `config.yaml`.

## Setup

1. Create `.env` with your connection info.
2. Adjust `config.yaml` to match your API query parameters, required fields, and output preferences.

Example `.env`:

```
API_BASE_URL=https://api.dev.aipass.jp/public
API_TOKEN=your_token_here
API_TOKEN_HEADER=Authorization
API_EMAIL=your_account_email_here
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
- If you only have an email/password (no API token), set `API_EMAIL` (or `API_USERNAME`) and `API_PASSWORD` and leave `API_TOKEN` empty (or keep the placeholder `your_token_here`). The script will request an access token from `{API_BASE_URL}/oauth/token` by default.
- To override the token endpoint, set `API_AUTH_URL` explicitly (e.g., `https://api.dev.aipass.jp/public/oauth/token`).
