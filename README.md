# DB Data Pusher

Retrieving data from PostgreSQL table and sending it to PushGateway.

## Installation

1. Install uv:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

2. Create a virtual environment and install dependencies:
```bash
uv venv
source .venv/bin/activate
uv pip install -e .
```

3. Copy `.env.example` to `.env` and configure environment variables:
```bash
cp .env.example .env
```

## Running

```bash
python main.py
```

## Requirements

- Python 3.9+
- PostgreSQL
- PushGateway 