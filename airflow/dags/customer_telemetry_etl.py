"""Airflow DAG prototype for building a customer telemetry mart in ClickHouse.

CRM database (connection id: ``postgres_crm``)
---------------------------------------------
````
CREATE TABLE crm.customers (
    customer_id      SERIAL PRIMARY KEY,
    external_id      VARCHAR(64) NOT NULL,
    full_name        VARCHAR(255) NOT NULL,
    email            VARCHAR(255),
    signup_channel   VARCHAR(64),
    signup_date      DATE NOT NULL
);
````

Telemetry database (connection id: ``postgres_telemetry``)
----------------------------------------------------------
````
CREATE TABLE telemetry.device_events (
    event_id        BIGSERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    device_id       VARCHAR(64) NOT NULL,
    event_ts        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    session_seconds INTEGER NOT NULL,
    error_code      VARCHAR(32),
    payload_size_kb INTEGER NOT NULL,
    signal_strength NUMERIC(5, 2)
);
````

Target ClickHouse database (connection id: ``clickhouse_dw``)
-------------------------------------------------------------
The ETL process populates the ``analytics.customer_telemetry_daily`` table:
````
CREATE TABLE analytics.customer_telemetry_daily (
    event_date           Date,
    customer_id          UInt32,
    external_id          String,
    full_name            String,
    signup_channel       LowCardinality(String),
    total_sessions       UInt32,
    total_session_time   UInt64,
    avg_session_time     Float64,
    errors_count         UInt32,
    total_payload_mb     Float64,
    avg_signal_strength  Float64,
    updated_at           DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (event_date, customer_id);
````

"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Tuple

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

CRM_QUERY = """
SELECT
    customer_id,
    external_id,
    full_name,
    email,
    signup_channel,
    signup_date
FROM crm.customers
WHERE signup_date <= %(ds)s::date
"""

TELEMETRY_QUERY = """
SELECT
    customer_id,
    date_trunc('day', event_ts) AS event_date,
    COUNT(*) AS total_sessions,
    SUM(session_seconds) AS total_session_time,
    AVG(session_seconds) AS avg_session_time,
    COUNT(CASE WHEN error_code IS NOT NULL THEN 1 END) AS errors_count,
    SUM(payload_size_kb) / 1024.0 AS total_payload_mb,
    AVG(signal_strength) AS avg_signal_strength
FROM telemetry.device_events
WHERE event_ts >= %(start_ts)s::timestamp
  AND event_ts < %(end_ts)s::timestamp
GROUP BY customer_id, event_date
"""


def _dictify(columns: Iterable[str], rows: Iterable[Tuple]) -> List[Dict]:
    """Convert a set of rows (tuples) to dictionaries keyed by ``columns``."""

    return [dict(zip(columns, row)) for row in rows]


def extract_crm_data(execution_date: datetime, **_: Dict) -> List[Dict]:
    """Fetch customers from the CRM system using the ``postgres_crm`` connection."""

    hook = PostgresHook(postgres_conn_id="postgres_crm")
    records = hook.get_records(
        CRM_QUERY,
        parameters={"ds": execution_date.strftime("%Y-%m-%d")},
    )
    columns = (
        "customer_id",
        "external_id",
        "full_name",
        "email",
        "signup_channel",
        "signup_date",
    )
    return _dictify(columns, records)


def extract_telemetry_data(execution_date: datetime, **_: Dict) -> List[Dict]:
    """Aggregate telemetry events by customer and day."""

    hook = PostgresHook(postgres_conn_id="postgres_telemetry")
    start_ts = (execution_date - timedelta(days=1)).replace(hour=0, minute=0, second=0)
    end_ts = execution_date.replace(hour=0, minute=0, second=0)
    records = hook.get_records(
        TELEMETRY_QUERY,
        parameters={
            "start_ts": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "end_ts": end_ts.strftime("%Y-%m-%d %H:%M:%S"),
        },
    )
    columns = (
        "customer_id",
        "event_date",
        "total_sessions",
        "total_session_time",
        "avg_session_time",
        "errors_count",
        "total_payload_mb",
        "avg_signal_strength",
    )
    return _dictify(columns, records)


def transform_customer_telemetry(**context: Dict) -> List[Dict]:
    """Enrich telemetry aggregates with CRM data.
    """

    ti = context["ti"]
    crm_rows: List[Dict] = ti.xcom_pull(task_ids="extract_crm") or []
    telemetry_rows: List[Dict] = ti.xcom_pull(task_ids="extract_telemetry") or []

    crm_index = {row["customer_id"]: row for row in crm_rows}
    telemetry_index: Dict[int, Dict[str, Dict]] = defaultdict(dict)

    for row in telemetry_rows:
        customer_id = row["customer_id"]
        event_date = row["event_date"].date() if hasattr(row["event_date"], "date") else row["event_date"]
        telemetry_index[customer_id][event_date] = row

    enriched_rows: List[Dict] = []

    for customer_id, crm_row in crm_index.items():
        customer_telemetry = telemetry_index.get(customer_id)

        if not customer_telemetry:
            enriched_rows.append(
                {
                    "event_date": context["execution_date"].date(),
                    "customer_id": customer_id,
                    "external_id": crm_row["external_id"],
                    "full_name": crm_row["full_name"],
                    "signup_channel": crm_row["signup_channel"],
                    "total_sessions": 0,
                    "total_session_time": 0,
                    "avg_session_time": 0.0,
                    "errors_count": 0,
                    "total_payload_mb": 0.0,
                    "avg_signal_strength": None,
                }
            )
            continue

        for event_date, telemetry_row in customer_telemetry.items():
            avg_signal = telemetry_row.get("avg_signal_strength")
            enriched_rows.append(
                {
                    "event_date": event_date,
                    "customer_id": customer_id,
                    "external_id": crm_row["external_id"],
                    "full_name": crm_row["full_name"],
                    "signup_channel": crm_row["signup_channel"],
                    "total_sessions": telemetry_row["total_sessions"],
                    "total_session_time": telemetry_row["total_session_time"],
                    "avg_session_time": float(telemetry_row["avg_session_time"] or 0.0),
                    "errors_count": telemetry_row["errors_count"],
                    "total_payload_mb": float(telemetry_row["total_payload_mb"] or 0.0),
                    "avg_signal_strength": None if avg_signal is None else float(avg_signal),
                }
            )

    return enriched_rows


def load_into_clickhouse(**context: Dict) -> None:
    """Generate the SQL required to load aggregated data into ClickHouse.
    """

    ti = context["ti"]
    aggregated_rows: List[Dict] = ti.xcom_pull(task_ids="transform") or []
    if not aggregated_rows:
        return

    conn = BaseHook.get_connection("clickhouse_dw")
    values_clause = ",\n".join(
        "(" + ", ".join([
            f"toDate('{row['event_date']}')",
            str(row["customer_id"]),
            f"'{row['external_id']}'",
            f"'{row['full_name'].replace("'", "''")}'",
            f"'{(row['signup_channel'] or '').replace("'", "''")}'",
            str(row["total_sessions"]),
            str(row["total_session_time"]),
            f"{row['avg_session_time']:.2f}",
            str(row["errors_count"]),
            f"{row['total_payload_mb']:.4f}",
            "NULL" if row["avg_signal_strength"] is None else f"{row['avg_signal_strength']:.2f}",
            "now()",
        ]) + ")"
        for row in aggregated_rows
    )

    insert_sql = f"""
    INSERT INTO analytics.customer_telemetry_daily (
        event_date,
        customer_id,
        external_id,
        full_name,
        signup_channel,
        total_sessions,
        total_session_time,
        avg_session_time,
        errors_count,
        total_payload_mb,
        avg_signal_strength,
        updated_at
    ) VALUES\n{values_clause}
    """.strip()

    # The log message makes the intent explicit for teams reviewing the prototype.
    ti.log.info(
        "ClickHouse host=%s database=%s -- SQL to execute:\n%s",
        conn.host,
        conn.schema or "default",
        insert_sql,
    )


def _doc_md() -> str:
    return """
### ETL Flow
"""


def build_dag() -> DAG:
    with DAG(
        dag_id="customer_telemetry_etl",
        description="Prototype ETL that prepares a ClickHouse mart with CRM and telemetry data",
        start_date=datetime(2024, 1, 1),
        schedule="0 2 * * *",
        catchup=False,
        default_args={
            "owner": "data-platform",
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=10),
        },
        tags=["prototype", "etl", "clickhouse"],
        doc_md=_doc_md(),
    ) as dag:
        start = EmptyOperator(task_id="start")

        extract_crm_task = PythonOperator(
            task_id="extract_crm",
            python_callable=extract_crm_data,
        )

        extract_telemetry_task = PythonOperator(
            task_id="extract_telemetry",
            python_callable=extract_telemetry_data,
        )

        transform_task = PythonOperator(
            task_id="transform",
            python_callable=transform_customer_telemetry,
        )

        load_task = PythonOperator(
            task_id="load",
            python_callable=load_into_clickhouse,
        )

        finish = EmptyOperator(task_id="finish")

        start >> [extract_crm_task, extract_telemetry_task]
        [extract_crm_task, extract_telemetry_task] >> transform_task >> load_task >> finish

    return dag


globals()["customer_telemetry_etl"] = build_dag()