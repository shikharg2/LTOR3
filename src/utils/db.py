import os
import time
import uuid
import threading
from datetime import datetime, timezone
from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from src.utils.error_logger import log_error


def get_connection_params() -> dict:
    """Get database connection parameters from environment variables."""
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "dbname": os.getenv("DB_NAME", "postgres"),
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "postgres"),
        "connect_timeout": 10,
    }


_pool = None
_pool_lock = threading.Lock()


def _get_pool() -> ThreadedConnectionPool:
    """Get or create the shared connection pool (thread-safe)."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                params = get_connection_params()
                _pool = ThreadedConnectionPool(minconn=2, maxconn=10, **params)
    return _pool


def _reset_pool():
    """Close all connections in the pool and force re-creation on next use."""
    global _pool
    with _pool_lock:
        if _pool is not None:
            try:
                _pool.closeall()
            except Exception:
                pass
            _pool = None


def _is_conn_alive(conn) -> bool:
    """Check whether a pooled connection is still usable."""
    try:
        if conn.closed:
            return False
        # Lightweight round-trip to verify the server is reachable
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.close()
        # Reset any transaction state the health check may have opened
        conn.rollback()
        return True
    except Exception:
        return False


@contextmanager
def get_connection(max_retries: int = 3, retry_delay: float = 1.0):
    """Context manager for database connections using connection pool with retry logic."""
    conn = None
    acquired_pool = None
    for attempt in range(1, max_retries + 1):
        try:
            acquired_pool = _get_pool()
            conn = acquired_pool.getconn()
            # Verify the connection is actually alive; stale pooled
            # connections cause "connection already closed" errors.
            if not _is_conn_alive(conn):
                try:
                    acquired_pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = None
                acquired_pool = None
                _reset_pool()
                raise psycopg2.OperationalError("Stale connection detected, resetting pool")
            conn.autocommit = False
            break
        except psycopg2.pool.PoolError as e:
            if attempt < max_retries:
                log_error("db", "get_connection",
                          Exception(f"Pool exhausted, retrying ({attempt}/{max_retries})"))
                time.sleep(retry_delay * attempt)
            else:
                log_error("db", "get_connection", e)
                raise
        except psycopg2.OperationalError as e:
            if attempt < max_retries:
                log_error("db", "get_connection",
                          Exception(f"Connection failed, retrying ({attempt}/{max_retries}): {e}"))
                _reset_pool()
                acquired_pool = None
                time.sleep(retry_delay * attempt)
            else:
                log_error("db", "get_connection", e)
                raise
    try:
        yield conn
        conn.commit()
    except Exception as e:
        if conn and not conn.closed:
            conn.rollback()
        log_error("db", "get_connection", e)
        raise
    finally:
        if conn is not None:
            try:
                if acquired_pool is not None:
                    if conn.closed:
                        acquired_pool.putconn(conn, close=True)
                    else:
                        acquired_pool.putconn(conn)
                else:
                    # Pool was reset; close the orphaned connection directly
                    if not conn.closed:
                        conn.close()
            except Exception:
                try:
                    if not conn.closed:
                        conn.close()
                except Exception:
                    pass


def ensure_schema_migrations() -> None:
    """Apply schema migrations for columns added after initial release."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'load_test' AND table_name = 'scenarios'
                          AND column_name = 'config_name'
                    ) THEN
                        ALTER TABLE load_test.scenarios ADD COLUMN config_name VARCHAR(255);
                        CREATE INDEX IF NOT EXISTS idx_scenarios_config_name
                            ON load_test.scenarios(config_name);
                    END IF;
                END $$;
                """
            )


def insert_scenario(scenario_id: str, protocol: str, config_snapshot: dict,
                    config_name: str = None) -> None:
    """Insert a new scenario into the scenarios table."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO load_test.scenarios (scenario_id, protocol, config_snapshot, config_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (scenario_id) DO UPDATE SET
                    config_snapshot = EXCLUDED.config_snapshot,
                    config_name = EXCLUDED.config_name
                """,
                (scenario_id, protocol, psycopg2.extras.Json(config_snapshot), config_name)
            )


def insert_test_run(run_id: str, scenario_id: str, start_time: datetime, worker_node: str) -> None:
    """Insert a new test run into the test_runs table."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO load_test.test_runs (run_id, scenario_id, start_time, worker_node)
                VALUES (%s, %s, %s, %s)
                """,
                (run_id, scenario_id, start_time, worker_node)
            )


def insert_raw_metric(run_id: str, metric_name: str, metric_value: str) -> None:
    """Insert a raw metric into the raw_metrics table."""
    metric_id = str(uuid.uuid4())
    timestamp = datetime.now(timezone.utc)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO load_test.raw_metrics (id, run_id, metric_name, metric_value, timestamp)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (metric_id, run_id, metric_name, metric_value, timestamp)
            )


def insert_raw_metrics_batch(run_id: str, metrics: dict[str, float]) -> None:
    """Insert multiple raw metrics in a single transaction."""
    timestamp = datetime.now(timezone.utc)
    with get_connection() as conn:
        with conn.cursor() as cur:
            for metric_name, metric_value in metrics.items():
                metric_id = str(uuid.uuid4())
                cur.execute(
                    """
                    INSERT INTO load_test.raw_metrics (id, run_id, metric_name, metric_value, timestamp)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (metric_id, run_id, metric_name, str(metric_value), timestamp)
                )


def insert_result_log(run_id: str, metric_name: str, expected_value: str,
                      measured_value: str, status: str, scope: str) -> None:
    """Insert a result log entry."""
    result_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO load_test.results_log (id, run_id, metric_name, expected_value, measured_value, status, scope)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (result_id, run_id, metric_name, expected_value, measured_value, status, scope)
            )


def get_raw_metrics_for_run(run_id: str) -> list[dict]:
    """Get all raw metrics for a specific run."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT metric_name, metric_value, timestamp
                FROM load_test.raw_metrics
                WHERE run_id = %s
                """,
                (run_id,)
            )
            return cur.fetchall()


def get_raw_metrics_for_scenario(scenario_id: str) -> list[dict]:
    """Get all raw metrics for a scenario (across all runs)."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT rm.metric_name, rm.metric_value::NUMERIC as metric_value, rm.timestamp
                FROM load_test.raw_metrics rm
                JOIN load_test.test_runs tr ON rm.run_id = tr.run_id
                WHERE tr.scenario_id = %s
                """,
                (scenario_id,)
            )
            return cur.fetchall()


def insert_scenario_summary(scenario_id: str, metric_name: str, sample_count: int,
                            avg_value: float, min_value: float, max_value: float,
                            percentile: int, percentile_result: float, stddev_value: float) -> None:
    """
    Insert or update scenario summary.

    Args:
        scenario_id: UUID of the scenario
        metric_name: Name of the metric
        sample_count: Number of samples
        avg_value: Average value
        min_value: Minimum value
        max_value: Maximum value
        percentile: User-specified percentile value (1-99), e.g., 99 for p99
        percentile_result: The calculated result for that percentile
        stddev_value: Standard deviation
    """
    summary_id = str(uuid.uuid4())
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO load_test.scenario_summary
                (id, scenario_id, metric_name, sample_count, avg_value, min_value, max_value, percentile, percentile_result, stddev_value, aggregated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (scenario_id, metric_name) DO UPDATE SET
                    sample_count = EXCLUDED.sample_count,
                    avg_value = EXCLUDED.avg_value,
                    min_value = EXCLUDED.min_value,
                    max_value = EXCLUDED.max_value,
                    percentile = EXCLUDED.percentile,
                    percentile_result = EXCLUDED.percentile_result,
                    stddev_value = EXCLUDED.stddev_value,
                    aggregated_at = NOW()
                """,
                (summary_id, scenario_id, metric_name, sample_count, avg_value,
                 min_value, max_value, percentile, percentile_result, stddev_value)
            )


def get_latest_scenario_ids() -> list[str]:
    """Return scenario_ids ordered by most recent test run (newest first)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT s.scenario_id
                FROM load_test.scenarios s
                LEFT JOIN load_test.test_runs tr ON s.scenario_id = tr.scenario_id
                GROUP BY s.scenario_id
                ORDER BY MAX(tr.start_time) DESC NULLS LAST
                """
            )
            return [str(row[0]) for row in cur.fetchall()]


def get_scenarios(scenario_ids: list[str] = None) -> list[dict]:
    """Return scenario metadata. Optionally filter by scenario_ids."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if scenario_ids:
                cur.execute(
                    """
                    SELECT scenario_id, protocol, config_snapshot, config_name
                    FROM load_test.scenarios
                    WHERE scenario_id = ANY(%s::uuid[])
                    """,
                    (scenario_ids,)
                )
            else:
                cur.execute("SELECT scenario_id, protocol, config_snapshot, config_name FROM load_test.scenarios")
            return cur.fetchall()


def get_distinct_config_names() -> list[dict]:
    """Return distinct config names with a representative scenario_id for each."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT config_name,
                       (ARRAY_AGG(s.scenario_id ORDER BY tr.latest DESC NULLS LAST))[1] AS scenario_id
                FROM load_test.scenarios s
                LEFT JOIN (
                    SELECT scenario_id, MAX(start_time) AS latest
                    FROM load_test.test_runs GROUP BY scenario_id
                ) tr ON s.scenario_id = tr.scenario_id
                GROUP BY config_name
                ORDER BY MAX(tr.latest) DESC NULLS LAST
                """
            )
            return cur.fetchall()


def get_test_runs(scenario_id: str, start_dt: datetime = None, end_dt: datetime = None) -> list[dict]:
    """Return test runs for a scenario with optional date filtering."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = "SELECT run_id, scenario_id, start_time, worker_node FROM load_test.test_runs WHERE scenario_id = %s"
            params: list = [scenario_id]
            if start_dt:
                query += " AND start_time >= %s"
                params.append(start_dt)
            if end_dt:
                query += " AND start_time <= %s"
                params.append(end_dt)
            query += " ORDER BY start_time"
            cur.execute(query, params)
            return cur.fetchall()


def get_result_logs(scenario_id: str, start_dt: datetime = None, end_dt: datetime = None) -> list[dict]:
    """Return result logs for a scenario with optional date filtering via test_runs join."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            query = """
                SELECT rl.id, rl.run_id, rl.metric_name, rl.expected_value,
                       rl.measured_value, rl.status, rl.scope,
                       tr.start_time, tr.scenario_id
                FROM load_test.results_log rl
                JOIN load_test.test_runs tr ON rl.run_id = tr.run_id
                WHERE tr.scenario_id = %s
            """
            params: list = [scenario_id]
            if start_dt:
                query += " AND tr.start_time >= %s"
                params.append(start_dt)
            if end_dt:
                query += " AND tr.start_time <= %s"
                params.append(end_dt)
            query += " ORDER BY tr.start_time, rl.metric_name"
            cur.execute(query, params)
            return cur.fetchall()


def get_scenario_summaries(scenario_id: str) -> list[dict]:
    """Return scenario summary rows for a scenario."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, scenario_id, metric_name, sample_count,
                       avg_value, min_value, max_value, percentile,
                       percentile_result, stddev_value, aggregated_at
                FROM load_test.scenario_summary
                WHERE scenario_id = %s
                ORDER BY metric_name
                """,
                (scenario_id,)
            )
            return cur.fetchall()


def export_filtered_to_csv(scenario_ids: list[str], start_dt: datetime,
                           end_dt: datetime, output_dir: str) -> None:
    """Export filtered data to CSV files in output_dir."""
    _export_with_filters(scenario_ids, start_dt, end_dt, output_dir)


def _export_with_filters(scenario_ids: list[str], start_dt: datetime,
                         end_dt: datetime, output_dir: str) -> None:
    """Internal: export filtered data using queries and csv module."""
    import csv as csv_mod
    os.makedirs(output_dir, exist_ok=True)

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Scenarios
            cur.execute(
                "SELECT scenario_id, protocol, config_snapshot FROM load_test.scenarios WHERE scenario_id = ANY(%s::uuid[]) ORDER BY scenario_id",
                (scenario_ids,)
            )
            _write_csv(output_dir, "scenarios.csv", ["scenario_id", "protocol", "config_snapshot"], cur.fetchall())

            # Test runs
            cur.execute(
                "SELECT run_id, scenario_id, start_time, worker_node FROM load_test.test_runs WHERE scenario_id = ANY(%s::uuid[]) AND start_time >= %s AND start_time <= %s ORDER BY start_time",
                (scenario_ids, start_dt, end_dt)
            )
            runs = cur.fetchall()
            _write_csv(output_dir, "test_runs.csv", ["run_id", "scenario_id", "start_time", "worker_node"], runs)

            run_ids = [r[0] for r in runs]
            if run_ids:
                # Raw metrics
                cur.execute(
                    "SELECT id, run_id, metric_name, metric_value, timestamp FROM load_test.raw_metrics WHERE run_id = ANY(%s::uuid[]) ORDER BY timestamp",
                    (run_ids,)
                )
                _write_csv(output_dir, "raw_metrics.csv", ["id", "run_id", "metric_name", "metric_value", "timestamp"], cur.fetchall())

                # Results log
                cur.execute(
                    "SELECT id, run_id, metric_name, expected_value, measured_value, status, scope FROM load_test.results_log WHERE run_id = ANY(%s::uuid[]) ORDER BY id",
                    (run_ids,)
                )
                _write_csv(output_dir, "results_log.csv", ["id", "run_id", "metric_name", "expected_value", "measured_value", "status", "scope"], cur.fetchall())

            # Scenario summary
            cur.execute(
                "SELECT * FROM load_test.scenario_summary WHERE scenario_id = ANY(%s::uuid[]) ORDER BY metric_name",
                (scenario_ids,)
            )
            cols = [desc[0] for desc in cur.description]
            _write_csv(output_dir, "scenario_summary.csv", cols, cur.fetchall())


def _write_csv(output_dir: str, filename: str, headers: list, rows: list) -> None:
    """Write rows to a CSV file."""
    import csv as csv_mod
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([str(v) if v is not None else "" for v in row])
