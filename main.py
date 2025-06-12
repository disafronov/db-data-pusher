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
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASS = os.getenv("DB_PASS")
        DB_PORT = int(os.getenv("DB_PORT") or "5432")
        DB_TIMEOUT = int(os.getenv("DB_TIMEOUT") or "5")

        # Table configuration
        TABLE_NAME = os.getenv("TABLE_NAME")
        ID_COLUMN = os.getenv("ID_COLUMN") or "id"
        VALUE_COLUMN = os.getenv("VALUE_COLUMN") or "value"
        UPDATEDON_COLUMN = os.getenv("UPDATEDON_COLUMN") or "updatedon"

        # PushGateway configuration
        PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
        HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT") or "10")

        # Metric names configuration
        JOB_NAME = sanitize(os.getenv("JOB_NAME") or f"{DB_NAME}_{TABLE_NAME}")
        INSTANCE_NAME = sanitize(os.getenv("INSTANCE_NAME") or JOB_NAME)

        required = {
            "DB_HOST": DB_HOST,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASS": DB_PASS,
            "TABLE_NAME": TABLE_NAME,
            "PUSHGATEWAY_URL": PUSHGATEWAY_URL,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        # Connect to database and execute query
        query = sql.SQL("SELECT {}, {}, {} FROM {}").format(
            sql.Identifier(ID_COLUMN),
            sql.Identifier(VALUE_COLUMN),
            sql.Identifier(UPDATEDON_COLUMN),
            sql.Identifier(TABLE_NAME),
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
        metric_prefix = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}"
        lines = [
            f"# HELP {metric_prefix}_value Value from {TABLE_NAME}",
            f"# TYPE {metric_prefix}_value gauge",
            f"# HELP {metric_prefix}_updatedon Last update timestamp from {TABLE_NAME}",
            f"# TYPE {metric_prefix}_updatedon gauge",
            f"# HELP {metric_prefix}_total_rows Total rows in {TABLE_NAME}",
            f"# TYPE {metric_prefix}_total_rows gauge",
            f"{metric_prefix}_total_rows {len(rows)}"
        ]

        count = 1  # total_rows metric

        for id_, value, updatedon in rows:
            if not id_:
                logger.warning("Skipping row with empty ID")
                continue
            sid = sanitize(str(id_))
            if value is not None:
                lines.append(f'{metric_prefix}_value{{id="{sid}"}} {value}')
                count += 1
            if updatedon and isinstance(updatedon, datetime):
                ts = int(updatedon.timestamp())
                lines.append(f'{metric_prefix}_updatedon{{id="{sid}"}} {ts}')
                count += 1

        # Push metrics
        url = f"{PUSHGATEWAY_URL}/metrics/job/{JOB_NAME}/instance/{INSTANCE_NAME}"
        logger.info(f"Pushing {count} metrics to {url}")
        response = requests.post(url, data="\n".join(lines) + "\n", timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        logger.info("Metrics pushed successfully")

    except Exception as e:
        logger.error(f"Error: {sanitize_error(str(e))}")
        sys.exit(1)

if __name__ == "__main__":
    main()
