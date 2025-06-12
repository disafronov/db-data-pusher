#!/usr/bin/env python3
"""
Push database metrics to Prometheus PushGateway.
"""

import os
import sys
import psycopg2
import requests
import re
import logging
from psycopg2 import sql
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanitize(name: str) -> str:
    """Replace non-alphanumeric chars with underscore."""
    return re.sub(r'[^a-zA-Z0-9_]', '_', str(name))

def sanitize_error(error: str) -> str:
    """Remove database password from error messages."""
    # Handle different quote styles in password errors
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
        DB_PORT = int(os.getenv("DB_PORT", "5432"))

        # Table configuration
        TABLE_NAME = os.getenv("TABLE_NAME")
        ID_COLUMN = os.getenv("ID_COLUMN", "id")
        VALUE_COLUMN = os.getenv("VALUE_COLUMN", "value")
        UPDATEDON_COLUMN = os.getenv("UPDATEDON_COLUMN", "updatedon")

        # PushGateway configuration
        PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL")
        HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "10"))

        # Validate required environment variables
        required_env = {
            "DB_HOST": DB_HOST,
            "DB_NAME": DB_NAME,
            "DB_USER": DB_USER,
            "DB_PASS": DB_PASS,
            "TABLE_NAME": TABLE_NAME,
            "PUSHGATEWAY_URL": PUSHGATEWAY_URL
        }

        missing = [k for k, v in required_env.items() if not v]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        # Metric names configuration
        DEFAULT_NAME = f"{sanitize(DB_NAME)}_{sanitize(TABLE_NAME)}"
        JOB_NAME = sanitize(os.getenv("JOB_NAME", DEFAULT_NAME))
        INSTANCE_NAME = sanitize(os.getenv("INSTANCE_NAME", JOB_NAME))

        if not JOB_NAME or not INSTANCE_NAME:
            raise ValueError("JOB_NAME and INSTANCE_NAME cannot be empty after sanitization")

        # Connect to DB
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()

        try:
            # Get data
            cur.execute(sql.SQL("SELECT {}, {}, {} FROM {}").format(
                sql.Identifier(ID_COLUMN),
                sql.Identifier(VALUE_COLUMN),
                sql.Identifier(UPDATEDON_COLUMN),
                sql.Identifier(TABLE_NAME)
            ))
            rows = cur.fetchall()

            # Build metrics
            value_metric = f"{DEFAULT_NAME}_value"
            updatedon_metric = f"{DEFAULT_NAME}_updatedon"
            count_metric = f"{DEFAULT_NAME}_total_rows"
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
                if not id_:
                    logger.warning("Skipping row with empty ID")
                    continue

                sanitized_id = sanitize(str(id_))
                if not sanitized_id:
                    logger.warning(f"Skipping row with empty ID after sanitization: {id_}")
                    continue

                # Send value metric if value is not None (including zero)
                if value is not None:
                    lines.append(f'{value_metric}{{id="{sanitized_id}"}} {value}')
                    metrics_count += 1
                # Send updatedon metric only if updatedon is not None and is datetime
                if updatedon is not None and isinstance(updatedon, datetime):
                    updatedon_ts = int(updatedon.timestamp())
                    lines.append(f'{updatedon_metric}{{id="{sanitized_id}"}} {updatedon_ts}')
                    metrics_count += 1

            # Push metrics
            url = f"{PUSHGATEWAY_URL}/metrics/job/{JOB_NAME}/instance/{INSTANCE_NAME}"
            logger.info(f"Pushing metrics to: {url}")
            response = requests.post(url, data="\n".join(lines) + "\n", timeout=HTTP_TIMEOUT)
            response.raise_for_status()

            logger.info(f"Pushed {metrics_count} metrics")

        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error: {sanitize_error(str(e))}")
        sys.exit(1)

if __name__ == "__main__":
    main()
