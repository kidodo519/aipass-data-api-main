# aipass data api exporter

## Overview

`main.py` fetches reservations, sales, and room data from the aipass API and saves them to CSV or JSON files.
The date ranges are split into `history` and `onhand`, and the output destination (local folder and/or S3) is configured in `config.yaml`.

## Setup

1. Create `.env` with your connection info.
2. Adjust `config.yaml` to match your API query parameters, required fields, and output preferences.

Example `.env`:

```
API_BASE_URL=https://api.example.com/public
API_TOKEN=your_token_here
API_TOKEN_HEADER=Authorization
```

## Run

```
python main.py
```

## Notes

- The default date offsets are `history: -2 ~ -2` and `onhand: -1 ~ +178` from today.
- To override dates, set the `date_ranges.manual` values in `config.yaml`.
- When enabling S3 uploads, ensure AWS credentials are available via environment variables or AWS config files.
