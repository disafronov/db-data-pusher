#!/usr/bin/env python3
"""
Push database metrics to Prometheus PushGateway.
Reads data from PostgreSQL table and pushes metrics to PushGateway.

This script:
1. Connects to PostgreSQL database
2. Fetches data from specified table
3. Generates Prometheus metrics
4. Pushes metrics to PushGateway
5. Handles errors and retries
6. Provides graceful shutdown

Environment variables:
    Required:
        DB_CONN: PostgreSQL connection string
        TABLE_NAME: Name of the table to fetch data from
        PUSHGATEWAY_URL: URL of the PushGateway
        ID_FIELD: Name of the ID field
        VALUE_FIELD: Name of the value field
        UPDATEDON_FIELD: Name of the timestamp field

    Optional:
        JOB_NAME: Name of the job (default: dbname_tablename)
        DB_TIMEOUT: Database connection timeout in seconds (default: 5)
        HTTP_TIMEOUT: HTTP request timeout in seconds (default: 5)
        MAX_RETRIES: Maximum number of retry attempts (default: 3)
        RETRY_DELAY: Base delay between retries in seconds (default: 1)
        EXTRA_LABELS: Additional labels in comma-separated key=value format
"""

import os
import sys
import psycopg2
import requests
import logging
import json
import re
import time
import random
from datetime import datetime, timezone
from typing import List, Tuple, Optional, Any, Union, Callable
from psycopg2 import sql
import signal
from decimal import Decimal

# Constants
DEFAULT_TIMEOUT = 5
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY = 1
PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4"

# Database connection patterns
DB_NAME_PATTERNS = [
    r'//[^/]+/([^/?]+)',  # postgresql://user:pass@host/dbname
    r'@[^/]+/([^/?]+)',   # user:pass@host/dbname
    r'/([^/?]+)$'         # /dbname
]

# SQL identifier validation pattern
SQL_IDENTIFIER_PATTERN = r'^[a-zA-Z0-9_]+$'

def sanitize_name(name: str) -> str:
    """Sanitize name for Prometheus metric names and labels.
    
    Args:
        name: Name to sanitize
        
    Returns:
        Sanitized name with only alphanumeric characters and underscores
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

def escape_label_value(value: Any) -> str:
    """Escape label value for Prometheus metrics.
    
    Args:
        value: Value to escape
        
    Returns:
        Escaped string value safe for Prometheus labels
    """
    return (str(value)
            .replace('\\', '\\\\')
            .replace('\n', '\\n')
            .replace('\t', '\\t')
            .replace('"', '\\"'))

class JsonFormatter(logging.Formatter):
    """Custom JSON formatter for logs with millisecond precision."""
    def format(self, record: logging.LogRecord) -> str:
        timestamp = self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%f")[:-3] + 'Z'
        log_record = {
            "timestamp": timestamp,
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "job": JOB_NAME,
            "db": DB_NAME,
            "table": TABLE_NAME,
        }
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logger() -> logging.Logger:
    """Configure JSON logger with millisecond precision."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(handler)
    return logger

logger = setup_logger()

def log_event(event_type: str, **kwargs) -> None:
    """Log structured event with additional context.
    
    Args:
        event_type: Type of the event (e.g., 'job_started', 'db_connected')
        **kwargs: Additional context to include in the log
    """
    log_data = {
        "event": event_type,
        **kwargs
    }
    logger.info(json.dumps(log_data))

def fail_and_exit(message: str, exc_info: Optional[Exception] = None) -> None:
    """Log error and exit with status code 1.
    
    Args:
        message: Error message to log
        exc_info: Optional exception info to include in log
    """
    logger.error(message, exc_info=exc_info)
    sys.exit(1)

def get_env_or_exit(var_name: str) -> str:
    """Get required environment variable or exit if not set.
    
    Args:
        var_name: Name of the environment variable
        
    Returns:
        Value of the environment variable
        
    Raises:
        SystemExit: If environment variable is not set
    """
    value = os.getenv(var_name)
    if not value:
        fail_and_exit(f"Missing required environment variable '{var_name}'")
    return value

def get_db_name_from_conn(conn_string: str) -> str:
    """Extract database name from connection string.
    
    Args:
        conn_string: PostgreSQL connection string
        
    Returns:
        Database name
        
    Raises:
        SystemExit: If database name cannot be extracted
    """
    for pattern in DB_NAME_PATTERNS:
        match = re.search(pattern, conn_string)
        if match:
            return match.group(1)
    fail_and_exit("Failed to extract database name from connection string")

def validate_sql_identifier(name: str) -> str:
    """Validate SQL identifier to prevent SQL injection.
    
    Args:
        name: SQL identifier to validate
        
    Returns:
        Validated SQL identifier
        
    Raises:
        SystemExit: If identifier is invalid
    """
    if not re.match(SQL_IDENTIFIER_PATTERN, name):
        fail_and_exit(f"Invalid SQL identifier: {name}")
    return name

def parse_extra_labels(env_var: str = "EXTRA_LABELS") -> str:
    """Parse EXTRA_LABELS environment variable into Prometheus labels string.
    
    Args:
        env_var: Name of the environment variable containing extra labels
        
    Returns:
        Comma-separated string of valid label pairs
    """
    raw = os.getenv(env_var, "")
    labels = []
    for pair in raw.split(","):
        if not pair.strip():
            continue
        if "=" not in pair:
            logger.warning(f"Ignoring invalid label '{pair}' in {env_var}")
            continue
        key, value = pair.split("=", 1)
        key = key.strip()
        value = escape_label_value(value.strip())
        if not re.match(SQL_IDENTIFIER_PATTERN, key):
            logger.warning(f"Ignoring invalid label name '{key}' (must match {SQL_IDENTIFIER_PATTERN})")
            continue
        labels.append(f'{key}="{value}"')
    return ",".join(labels)

def get_metric_labels(job_name: str) -> str:
    """Get common metric labels including extra labels from env.
    
    Args:
        job_name: Name of the job for the job label
        
    Returns:
        Comma-separated string of all metric labels
    """
    base_labels = [
        f'job="{job_name}"',
        f'db="{DB_NAME}"',
        f'table="{TABLE_NAME}"'
    ]
    extra = parse_extra_labels()
    if extra:
        base_labels.append(extra)
    return ",".join(base_labels)

# Required environment variables
DB_CONN = get_env_or_exit("DB_CONN")
TABLE_NAME = validate_sql_identifier(get_env_or_exit("TABLE_NAME"))
PUSHGATEWAY_URL = get_env_or_exit("PUSHGATEWAY_URL")
ID_FIELD = validate_sql_identifier(get_env_or_exit("ID_FIELD"))
VALUE_FIELD = validate_sql_identifier(get_env_or_exit("VALUE_FIELD"))
UPDATEDON_FIELD = validate_sql_identifier(get_env_or_exit("UPDATEDON_FIELD"))

# Optional environment variables with defaults and validation
JOB_NAME = os.getenv("JOB_NAME", f"{sanitize_name(get_db_name_from_conn(DB_CONN))}_{sanitize_name(TABLE_NAME)}")
DB_TIMEOUT = validate_numeric_param("DB_TIMEOUT", os.getenv("DB_TIMEOUT", str(DEFAULT_TIMEOUT)))
HTTP_TIMEOUT = validate_numeric_param("HTTP_TIMEOUT", os.getenv("HTTP_TIMEOUT", str(DEFAULT_TIMEOUT)))
MAX_RETRIES = validate_numeric_param("MAX_RETRIES", os.getenv("MAX_RETRIES", str(DEFAULT_MAX_RETRIES)))
RETRY_DELAY = validate_numeric_param("RETRY_DELAY", os.getenv("RETRY_DELAY", str(DEFAULT_RETRY_DELAY)))

# Sanitized names for metrics
DB_NAME = get_db_name_from_conn(DB_CONN)
DB_NAME_S = sanitize_name(DB_NAME)
TABLE_NAME_S = sanitize_name(TABLE_NAME)

# Metric names
DB_CONNECT_SUCCESS_METRIC = 'db_connect_success'
DB_CONNECTION_DURATION_METRIC = 'db_connection_duration_seconds'
ROWS_COUNT_METRIC = 'rows_count'
VALUE_METRIC = 'value'
UPDATEDON_METRIC = 'updatedon'
SCRAPE_SUCCESS_METRIC = 'scrape_success'
SKIPPED_ROWS_METRIC = 'skipped_rows'
SCRAPE_DURATION_METRIC = 'scrape_duration_seconds'
LAST_SUCCESSFUL_SCRAPE_METRIC = 'last_successful_scrape_timestamp'
PUSH_ERRORS_METRIC = 'push_errors_total'

# SQL query using psycopg2.sql for safe identifier handling
QUERY = sql.SQL("SELECT {id}, {val}, {upd} FROM {tbl}").format(
    id=sql.Identifier(ID_FIELD),
    val=sql.Identifier(VALUE_FIELD),
    upd=sql.Identifier(UPDATEDON_FIELD),
    tbl=sql.Identifier(TABLE_NAME)
)

def validate_numeric_param(name: str, value: str, min_val: int = 1, max_val: int = 300) -> int:
    """Validate numeric parameter from environment variable.
    
    Args:
        name: Parameter name for error messages
        value: Parameter value to validate
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        Validated integer value
        
    Raises:
        SystemExit: If value is invalid or out of range
    """
    try:
        val = int(value)
        if val < min_val or val > max_val:
            fail_and_exit(f"Invalid {name}: {val}. Must be between {min_val} and {max_val}")
        return val
    except ValueError:
        fail_and_exit(f"Invalid {name}: {value}. Must be a number")

def fetch_rows(conn: psycopg2.extensions.connection) -> List[Tuple]:
    """Fetch rows from database with error handling and row limit.
    
    Args:
        conn: Database connection
        
    Returns:
        List of tuples containing row data, limited to MAX_ROWS
        
    Raises:
        SystemExit: If database error occurs
    """
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()
            if len(rows) > MAX_ROWS:
                logger.warning(f"Too many rows ({len(rows)}), limiting to {MAX_ROWS}")
                return rows[:MAX_ROWS]
            return rows
    except psycopg2.Error as e:
        fail_and_exit(f"Database error while fetching rows: {e}", exc_info=True)
    except Exception as e:
        fail_and_exit(f"Unexpected error while fetching rows: {e}", exc_info=True)

def generate_help_and_type() -> List[str]:
    """Generate HELP and TYPE comments for all metrics.
    
    Returns:
        List of HELP and TYPE comments for all metrics
    """
    return [
        f'# HELP {DB_CONNECT_SUCCESS_METRIC} Whether the database connection was successful',
        f'# TYPE {DB_CONNECT_SUCCESS_METRIC} gauge',
        f'# HELP {DB_CONNECTION_DURATION_METRIC} Time taken to connect to the database in seconds',
        f'# TYPE {DB_CONNECTION_DURATION_METRIC} gauge',
        f'# HELP {ROWS_COUNT_METRIC} Number of rows fetched',
        f'# TYPE {ROWS_COUNT_METRIC} gauge',
        f'# HELP {VALUE_METRIC} Value field from database',
        f'# TYPE {VALUE_METRIC} gauge',
        f'# HELP {UPDATEDON_METRIC} Timestamp of last update (Unix timestamp)',
        f'# TYPE {UPDATEDON_METRIC} gauge',
        f'# HELP {SCRAPE_SUCCESS_METRIC} Whether scraping was successful',
        f'# TYPE {SCRAPE_SUCCESS_METRIC} gauge',
        f'# HELP {SKIPPED_ROWS_METRIC} Number of rows skipped due to parsing errors',
        f'# TYPE {SKIPPED_ROWS_METRIC} gauge',
        f'# HELP {SCRAPE_DURATION_METRIC} Duration of scraping in seconds',
        f'# TYPE {SCRAPE_DURATION_METRIC} gauge',
        f'# HELP {LAST_SUCCESSFUL_SCRAPE_METRIC} Timestamp of last successful scrape',
        f'# TYPE {LAST_SUCCESSFUL_SCRAPE_METRIC} gauge',
        f'# HELP {PUSH_ERRORS_METRIC} Total number of push errors',
        f'# TYPE {PUSH_ERRORS_METRIC} counter',
    ]

def timestamp_to_unix_seconds(ts: Union[str, datetime, None]) -> Optional[float]:
    """Convert timestamp to Unix seconds (float).
    
    Args:
        ts: Timestamp string or datetime object
        
    Returns:
        Unix timestamp in seconds, or None if invalid
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.timestamp()
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None
    try:
        return float(ts)
    except (ValueError, TypeError):
        return None

def format_metric_line(name: str, labels: str, value: Any) -> str:
    """Format a single Prometheus metric line.
    
    Args:
        name: Metric name
        labels: Labels string inside {}
        value: Metric value
        
    Returns:
        Formatted metric line string
    """
    return f'{name}{{{labels}}} {value}'

def generate_summary_metrics(labels: str, skipped_rows: int, duration: float, db_connection_duration: float) -> List[str]:
    """Generate summary metrics lines.
    
    Args:
        labels: Labels string
        skipped_rows: Number of skipped rows
        duration: Total scrape duration
        db_connection_duration: DB connection duration
        
    Returns:
        List of metric lines
    """
    return [
        format_metric_line(DB_CONNECT_SUCCESS_METRIC, labels, 1),
        format_metric_line(SCRAPE_SUCCESS_METRIC, labels, 1),
        format_metric_line(SKIPPED_ROWS_METRIC, labels, skipped_rows),
        format_metric_line(SCRAPE_DURATION_METRIC, labels, f"{duration:.3f}"),
        format_metric_line(DB_CONNECTION_DURATION_METRIC, labels, f"{db_connection_duration:.3f}"),
        format_metric_line(LAST_SUCCESSFUL_SCRAPE_METRIC, labels, f"{time.time():.3f}"),
    ]

def generate_metrics_text(rows: List[Tuple], duration: float, db_connection_duration: float) -> Tuple[str, int]:
    """Generate full Prometheus metrics text from database rows.
    
    Args:
        rows: List of database rows (id, value, updatedon)
        duration: Total scrape duration
        db_connection_duration: DB connection duration in seconds
        
    Returns:
        Tuple of metrics text and count of skipped rows
    """
    labels = get_metric_labels(JOB_NAME)
    skipped_rows = 0
    lines = generate_help_and_type()

    for row in rows:
        if len(row) != 3:
            logger.warning(f"Skipping row with unexpected number of columns: {row}")
            skipped_rows += 1
            continue

        id_value, val, upd = row

        # Validate id_value
        if id_value is None:
            logger.warning(f"Skipping row with null ID: {row}")
            skipped_rows += 1
            continue

        id_str = escape_label_value(id_value)
        # Compose labels per metric
        per_row_labels = f'id="{id_str}"'
        # For possible extra labels, if any, they are already included in base labels

        # Validate numeric value
        try:
            val_num = float(val)
        except (TypeError, ValueError):
            logger.warning(f"Skipping row with non-numeric value '{val}' in row: {row}")
            skipped_rows += 1
            continue

        # Convert timestamp to unix float seconds
        upd_unix = timestamp_to_unix_seconds(upd)
        if upd_unix is None:
            logger.warning(f"Skipping row with invalid timestamp '{upd}' in row: {row}")
            skipped_rows += 1
            continue

        # Compose full labels string including base labels and per-row labels
        full_labels = ",".join([labels, per_row_labels])

        lines.append(format_metric_line(ROWS_COUNT_METRIC, full_labels, 1))
        lines.append(format_metric_line(VALUE_METRIC, full_labels, val_num))
        lines.append(format_metric_line(UPDATEDON_METRIC, full_labels, f"{upd_unix:.3f}"))

    lines.extend(generate_summary_metrics(labels, skipped_rows, duration, db_connection_duration))
    return "\n".join(lines) + "\n", skipped_rows

def push_metrics(pushgateway_url: str, job_name: str, metrics_text: str) -> None:
    """Push metrics text to Prometheus PushGateway with retries.
    
    Args:
        pushgateway_url: PushGateway base URL
        job_name: Job name label
        metrics_text: Prometheus metrics text
        
    Raises:
        SystemExit: If push fails after retries
    """
    url = f"{pushgateway_url}/metrics/job/{job_name}"
    headers = {'Content-Type': PROMETHEUS_CONTENT_TYPE}
    push_errors = 0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.put(url, data=metrics_text.encode('utf-8'), headers=headers, timeout=HTTP_TIMEOUT)
            if response.status_code == 202:
                logger.info(f"Metrics pushed successfully to {url}")
                return
            else:
                logger.warning(f"Unexpected response status {response.status_code} from PushGateway: {response.text}")
                push_errors += 1
        except requests.RequestException as e:
            logger.warning(f"Failed to push metrics on attempt {attempt}: {e}")
            push_errors += 1

        sleep_time = RETRY_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
        logger.info(f"Retrying after {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    # Add metric with push errors count
    metrics_text += f'\n{PUSH_ERRORS_METRIC}{{{get_metric_labels(JOB_NAME)}}} {push_errors}\n'
    fail_and_exit("Failed to push metrics after maximum retries")

def signal_handler(signum, frame) -> None:
    """Handle termination signals gracefully.
    
    Args:
        signum: Signal number
        frame: Current stack frame
    """
    logger.info(f"Received signal {signum}, exiting gracefully")
    sys.exit(0)

def main() -> None:
    """Main function that orchestrates the entire process."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    start_time = time.time()
    db_connection_duration = 0.0

    log_event("job_started")

    try:
        db_connect_start = time.time()
        with psycopg2.connect(DB_CONN, connect_timeout=DB_TIMEOUT) as conn:
            db_connection_duration = time.time() - db_connect_start
            log_event("db_connected", duration=db_connection_duration)
            rows = fetch_rows(conn)
            log_event("rows_fetched", count=len(rows))
    except Exception as e:
        log_event("db_error", error=str(e), exc_info=True)
        fail_and_exit(f"Failed to connect to database or fetch data: {e}", exc_info=True)

    duration = time.time() - start_time
    metrics_text, skipped = generate_metrics_text(rows, duration, db_connection_duration)
    log_event("metrics_generated", duration=duration, skipped_rows=skipped)

    try:
        push_metrics(PUSHGATEWAY_URL, JOB_NAME, metrics_text)
        log_event("job_completed", duration=time.time() - start_time)
    except Exception as e:
        log_event("job_failed", error=str(e), duration=time.time() - start_time, exc_info=True)
        raise

if __name__ == "__main__":
    main()
