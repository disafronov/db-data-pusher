#!/usr/bin/env python3
"""
Push database metrics to Prometheus PushGateway.
"""

import os
import sys
import time
import psycopg2
import requests
from psycopg2 import sql
from datetime import datetime

def main():
    """Main function to fetch data from DB and push metrics to PushGateway."""
    # Get environment variables
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    TABLE_NAME = os.getenv("TABLE_NAME")
    PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
    JOB_NAME = os.getenv("JOB_NAME", "pg_metrics_job")

    # Basic validation
    if not all([DB_HOST, DB_NAME, DB_USER, DB_PASS, TABLE_NAME, PUSHGATEWAY_URL]):
        print("Error: Missing required environment variables.", file=sys.stderr)
        sys.exit(1)

    try:
        # Connect to Postgres
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()

        # Build safe query
        query = sql.SQL("SELECT id, value, updatedon FROM {}").format(sql.Identifier(TABLE_NAME))
        cur.execute(query)

        # Fetch all rows
        rows = cur.fetchall()

        # Build Prometheus metrics text
        lines = [
            "# HELP table_value Value from table",
            "# TYPE table_value gauge"
        ]

        for row in rows:
            id_, value, updatedon = row
            if value is None or updatedon is None:
                continue
                
            timestamp = int(updatedon.timestamp()) if isinstance(updatedon, datetime) else int(time.time())
            lines.append(f'table_value{{id="{id_}"}} {value} {timestamp}')

        metrics_text = "\n".join(lines) + "\n"

        # Push metrics to PushGateway
        url = f"{PUSHGATEWAY_URL}/metrics/job/{JOB_NAME}"
        response = requests.post(url, data=metrics_text)
        response.raise_for_status()

        print(f"Successfully pushed {len(rows)} metrics")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main() 