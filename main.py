import os
import sys
import psycopg2
import requests
import logging
import json
import re

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logger():
    handler = logging.StreamHandler(sys.stdout)
    formatter = JsonFormatter()
    handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger

logger = setup_logger()

def fail_and_exit(message, exc_info=None):
    logger.error(message, exc_info=exc_info)
    sys.exit(1)

def get_env_or_exit(var_name):
    value = os.getenv(var_name)
    if not value:
        fail_and_exit(f"Failed to get required environment variable '{var_name}'")
    return value

def get_db_name_from_conn(conn_string):
    match = re.search(r'//[^/]+/([^/?]+)', conn_string)
    if not match:
        fail_and_exit("Failed to extract database name from connection string")
    return match.group(1)

DB_CONN = get_env_or_exit("DB_CONN")
DB_NAME = get_db_name_from_conn(DB_CONN)
TABLE_NAME = get_env_or_exit("TABLE_NAME")
PUSHGATEWAY_URL = get_env_or_exit("PUSHGATEWAY_URL")
ID_FIELD = get_env_or_exit("ID_FIELD")
VALUE_FIELD = get_env_or_exit("VALUE_FIELD")
UPDATEDON_FIELD = get_env_or_exit("UPDATEDON_FIELD")

def fetch_rows(conn):
    with conn.cursor() as cur:
        cur.execute(f"SELECT {ID_FIELD}, {VALUE_FIELD}, {UPDATEDON_FIELD} FROM {TABLE_NAME}")
        return cur.fetchall()

def push_metrics(rows):
    metrics = ""
    metrics += f'{DB_NAME}_{TABLE_NAME}_rows_count {len(rows)}\n'

    for id_, value, updatedon in rows:
        updatedon_ts = int(updatedon.timestamp())
        metrics += f'{DB_NAME}_{TABLE_NAME}_value{{{ID_FIELD}="{id_}"}} {value}\n'
        metrics += f'{DB_NAME}_{TABLE_NAME}_updatedon_seconds{{{ID_FIELD}="{id_}"}} {updatedon_ts}\n'

    response = requests.post(PUSHGATEWAY_URL, data=metrics.encode('utf-8'))
    return response

def main():
    try:
        conn = psycopg2.connect(DB_CONN)
    except Exception:
        fail_and_exit(f"Failed to establish database connection to '{DB_NAME}'", exc_info=True)

    try:
        rows = fetch_rows(conn)
    except Exception:
        fail_and_exit(f"Failed to fetch data from table '{TABLE_NAME}' in database '{DB_NAME}'", exc_info=True)

    try:
        response = push_metrics(rows)
        if response.status_code != 202:
            fail_and_exit(f"Failed to push metrics to PushGateway for database '{DB_NAME}': HTTP {response.status_code}, response: {response.text}")
    except Exception:
        fail_and_exit(f"Failed to push metrics to PushGateway for database '{DB_NAME}'", exc_info=True)

    logger.info(f"Successfully processed {len(rows)} rows from table '{TABLE_NAME}' in database '{DB_NAME}'")

if __name__ == "__main__":
    main()
