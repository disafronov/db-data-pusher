#!/usr/bin/env python3
"""
Push database metrics to Prometheus PushGateway.
"""

import os
import sys
import re
import logging
from datetime import datetime

import psycopg2
from psycopg2 import sql
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def sanitize(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name))

def sanitize_error(error: str) -> str:
    error = re.sub(r'password=([^"\s\']+)', 'password=***', error)
    error = re.sub(r'password="([^"]+)"', 'password="***"', error)
    error = re.sub(r"password='([^']+)'", "password='***'", error)
    return error

def main():
    try:
        # Database configuration
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = int(os.getenv("DB_PORT") or "5432")
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_TIMEOUT = int(os.getenv("DB_TIMEOUT") or "5")
        DB_TABLE = os.getenv("DB_TABLE")
        DB_TABLE_COLUMN_ID = os.getenv("DB_TABLE_COLUMN_ID") or "id"
        DB_TABLE_COLUMN_VALUE = os.getenv("DB_TABLE_COLUMN_VALUE") or "value"
        DB_TABLE_COLUMN_UPDATEDON = os.getenv("DB_TABLE_COLUMN_UPDATEDON") or "updatedon"

        # PushGateway configuration
        PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
        PUSHGATEWAY_TIMEOUT = int(os.getenv("PUSHGATEWAY_TIMEOUT") or "10")
        PUSHGATEWAY_JOB = os.getenv("PUSHGATEWAY_JOB")
        PUSHGATEWAY_INSTANCE = os.getenv("PUSHGATEWAY_INSTANCE")

        required = {
            "DB_HOST": DB_HOST,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASS": DB_PASS,
            "DB_TABLE": DB_TABLE,
            "PUSHGATEWAY_URL": PUSHGATEWAY_URL,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        # Connect to database and execute query
        query = sql.SQL("SELECT {}, {}, {} FROM {}").format(
            sql.Identifier(DB_TABLE_COLUMN_ID),
            sql.Identifier(DB_TABLE_COLUMN_VALUE),
            sql.Identifier(DB_TABLE_COLUMN_UPDATEDON),
            sql.Identifier(DB_TABLE),
        )
        with psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=DB_TIMEOUT
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()

        # Build metrics
        DEFAULT_NAME = f"{sanitize(DB_NAME)}_{sanitize(DB_TABLE)}"
        JOB_NAME = sanitize(PUSHGATEWAY_JOB or DEFAULT_NAME)
        INSTANCE_NAME = sanitize(PUSHGATEWAY_INSTANCE or JOB_NAME)

        metrics = {
            "value": {"type": "gauge", "help": f"Value from {DB_TABLE}"},
            "updatedon": {"type": "gauge", "help": f"Last update timestamp from {DB_TABLE}"},
            "total_rows": {"type": "gauge", "help": f"Total rows in {DB_TABLE}"},
        }

        lines = []
        for name, meta in metrics.items():
            lines.extend([
                f"# HELP {DEFAULT_NAME}_{name} {meta['help']}",
                f"# TYPE {DEFAULT_NAME}_{name} {meta['type']}"
            ])

        lines.append(f"{DEFAULT_NAME}_total_rows {len(rows)}")
        count = 1  # total_rows metric

        for id_, value, updatedon in rows:
            sid = sanitize(str(id_))
            if value is not None:
                lines.append(f'{DEFAULT_NAME}_value{{id="{sid}"}} {value}')
                count += 1
            if updatedon and isinstance(updatedon, datetime):
                ts = int(updatedon.timestamp())
                lines.append(f'{DEFAULT_NAME}_updatedon{{id="{sid}"}} {ts}')
                count += 1

        # Push metrics
        url = f"{PUSHGATEWAY_URL}/metrics/job/{JOB_NAME}/instance/{INSTANCE_NAME}"
        logger.info(f"Pushing {count} metrics to {url}")
        response = requests.post(url, data="\n".join(lines) + "\n", timeout=PUSHGATEWAY_TIMEOUT)
        response.raise_for_status()
        logger.info("Metrics pushed successfully")

    except Exception as e:
        logger.error(f"Error: {sanitize_error(str(e))}")
        sys.exit(1)

if __name__ == "__main__":
    main()
