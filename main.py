#!/usr/bin/env python3
"""
Push database metrics to Prometheus PushGateway.
"""

import os
import sys
import time
import psycopg2
import requests
import re
from psycopg2 import sql
from datetime import datetime

def sanitize(name: str) -> str:
    """Replace non-alphanumeric chars with underscore."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name))

def delete_old_metrics(pushgateway_url: str, job: str) -> None:
    """Delete old metrics for the given job across all instances."""
    url = f"{pushgateway_url}/metrics/job/{job}"
    try:
        requests.delete(url)
        print(f"Deleted old metrics for job={job}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to delete old metrics: {e}")

def main():
    # Get required env vars
    DB_HOST = os.getenv("DB_HOST")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    TABLE_NAME = os.getenv("TABLE_NAME")
    PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
    INSTANCE = os.getenv("INSTANCE")
    ID_COLUMN = os.getenv("ID_COLUMN", "id")
    VALUE_COLUMN = os.getenv("VALUE_COLUMN", "value")
    UPDATEDON_COLUMN = os.getenv("UPDATEDON_COLUMN", "updatedon")

    # Connect to DB
    conn = psycopg2.connect(
        host=DB_HOST, port=int(os.getenv("DB_PORT", "5432")),
        dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()

    # Get data
    cur.execute(sql.SQL("SELECT {}, {}, {} FROM {}").format(
        sql.Identifier(ID_COLUMN),
        sql.Identifier(VALUE_COLUMN),
        sql.Identifier(UPDATEDON_COLUMN),
        sql.Identifier(TABLE_NAME)
    ))
    rows = cur.fetchall()

    # Build metrics
    value_metric = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}_value"
    updatedon_metric = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}_updatedon"
    count_metric = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}_total_rows"
    lines = [
        f"# HELP {value_metric} Value from {TABLE_NAME}",
        f"# TYPE {value_metric} gauge",
        f"# HELP {updatedon_metric} Last update timestamp from {TABLE_NAME}",
        f"# TYPE {updatedon_metric} gauge",
        f"# HELP {count_metric} Total number of rows in {TABLE_NAME}",
        f"# TYPE {count_metric} gauge"
    ]

    # Add total rows count metric
    lines.append(f'{count_metric} {len(rows)}')

    metrics_count = 1  # Start with total_rows metric
    for id_, value, updatedon in rows:
        sanitized_id = sanitize(id_)
        # Send value metric if value is not None (including zero)
        if value is not None:
            lines.append(f'{value_metric}{{id="{sanitized_id}"}} {value}')
            metrics_count += 1
        # Send updatedon metric only if updatedon is not None
        if updatedon is not None:
            updatedon_ts = int(updatedon.timestamp())
            lines.append(f'{updatedon_metric}{{id="{sanitized_id}"}} {updatedon_ts}')
            metrics_count += 1

    # Delete old metrics before pushing new ones
    job_name = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}"
    delete_old_metrics(PUSHGATEWAY_URL, job_name)

    # Push metrics
    url = f"{PUSHGATEWAY_URL}/metrics/job/{job_name}/instance/{sanitize(INSTANCE)}"
    requests.post(url, data="\n".join(lines) + "\n")

    print(f"Pushed {metrics_count} metrics")

if __name__ == "__main__":
    main() 