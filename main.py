import os
import sys
import psycopg2
import requests
import logging
import json

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
        fail_and_exit(f"Environment variable {var_name} is required but not set")
    return value

DB_CONN = get_env_or_exit("DB_CONN")
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
    metrics += f'db_table_rows_count{{table="{TABLE_NAME}"}} {len(rows)}\n'

    for id_, value, updatedon in rows:
        updatedon_ts = int(updatedon.timestamp())
        metrics += f'db_table_value{{table="{TABLE_NAME}",{ID_FIELD}="{id_}"}} {value}\n'
        metrics += f'db_table_updatedon_seconds{{table="{TABLE_NAME}",{ID_FIELD}="{id_}"}} {updatedon_ts}\n'

    response = requests.post(PUSHGATEWAY_URL, data=metrics.encode('utf-8'))
    return response

def main():
    try:
        conn = psycopg2.connect(DB_CONN)
    except Exception:
        fail_and_exit("Database connection error", exc_info=True)

    try:
        rows = fetch_rows(conn)
    except Exception:
        fail_and_exit("Error fetching data from DB", exc_info=True)

    try:
        response = push_metrics(rows)
        if response.status_code != 202:
            fail_and_exit(f"PushGateway error: {response.status_code} {response.text}")
    except Exception:
        fail_and_exit("Error pushing metrics to PushGateway", exc_info=True)

    logger.info(f"Successfully pushed {len(rows)} rows.")

if __name__ == "__main__":
    main()
