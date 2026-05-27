"""
Airflow DAG: fraud_pipeline
Orchestrates the batch layer:
  1. data_quality_check  — Great Expectations suite on Bronze
  2. dbt_run             — transforms Bronze → Silver → Gold
  3. dbt_test            — runs all model tests
  4. freshness_check     — verifies Gold layer freshness SLA (< 1hr)
  5. slack_notify        — success/failure alert to #data-alerts

Schedule: every 30 minutes
SLA: Gold layer must be fresh within 60 minutes of source ingestion
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

SLACK_CONN_ID       = "slack_webhook_data_alerts"
DBT_PROJECT_DIR     = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR    = "/opt/airflow/dbt_profiles"
GE_SCRIPT           = "/opt/airflow/quality/expectations_suite.py"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "sla": timedelta(hours=1),
}

with DAG(
    dag_id="fraud_pipeline",
    default_args=default_args,
    description="Fraud detection pipeline — Bronze → Silver → Gold via dbt",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "data-engineering", "dbt"],
) as dag:

    # ── 1. Great Expectations data quality gate ────────────────────────────────
    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=(
            f"python {GE_SCRIPT} "
            "--run-id {{ run_id }} "
            "--fail-on-error"
        ),
        env={"SYNAPSE_CONN_STR": os.environ.get("SYNAPSE_CONN_STR", "")},
        doc_md="""
        Runs Great Expectations suite against Bronze transactions batch.
        Exits non-zero (and blocks downstream tasks) if any expectations fail.
        Failed records are already in dead-letter; this validates the valid set.
        """,
    )

    # ── 2. dbt run — Bronze → Silver → Gold ───────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} "
            "--target prod "
            "--select staging+ "   # run staging and everything downstream
            "--vars '{\"run_started_at\": \"{{ ts }}\"}'"
        ),
        doc_md="Runs all dbt models: staging → intermediate → marts.",
    )

    # ── 3. dbt test — schema + custom tests on all models ─────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR} "
            "--target prod "
            "--select staging+ "
            "--store-failures"     # persist failing rows to target schema for investigation
        ),
        doc_md="Runs all dbt tests. Failures are stored to target schema for debugging.",
    )

    # ── 4. Gold freshness check ────────────────────────────────────────────────
    def check_gold_freshness(**context) -> str:
        """
        Query Gold mart to verify max(mart_updated_at) is within 60 minutes.
        Returns branch task_id to route to success or freshness_sla_breach.
        """
        import pyodbc
        conn_str = os.environ["SYNAPSE_CONN_STR"]
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DATEDIFF(minute, MAX(mart_updated_at), GETUTCDATE())
                FROM marts.fct_fraud_events
            """)
            lag_minutes = cursor.fetchone()[0]

        context["ti"].xcom_push(key="gold_lag_minutes", value=lag_minutes)

        if lag_minutes is None or lag_minutes > 60:
            return "freshness_sla_breach"
        return "slack_success"

    freshness_check = BranchPythonOperator(
        task_id="freshness_check",
        python_callable=check_gold_freshness,
        provide_context=True,
    )

    # ── 5a. Slack — success ────────────────────────────────────────────────────
    slack_success = SlackWebhookOperator(
        task_id="slack_success",
        http_conn_id=SLACK_CONN_ID,
        message=(
            ":white_check_mark: *fraud_pipeline* succeeded\n"
            ">Run: `{{ run_id }}` | {{ ts }}\n"
            ">Gold lag: `{{ ti.xcom_pull(task_ids='freshness_check', key='gold_lag_minutes') }} min`"
        ),
        channel="#data-alerts",
    )

    # ── 5b. Slack — freshness SLA breach ──────────────────────────────────────
    freshness_sla_breach = SlackWebhookOperator(
        task_id="freshness_sla_breach",
        http_conn_id=SLACK_CONN_ID,
        message=(
            ":warning: *fraud_pipeline* FRESHNESS SLA BREACH\n"
            ">Run: `{{ run_id }}` | {{ ts }}\n"
            ">Gold lag exceeded 60 minutes: "
            "`{{ ti.xcom_pull(task_ids='freshness_check', key='gold_lag_minutes') }} min`\n"
            ">*Action required:* check Synapse SQL pool and dbt logs."
        ),
        channel="#data-alerts",
    )

    # ── 5c. Slack — pipeline failure ──────────────────────────────────────────
    slack_failure = SlackWebhookOperator(
        task_id="slack_failure",
        http_conn_id=SLACK_CONN_ID,
        message=(
            ":rotating_light: *fraud_pipeline* FAILED\n"
            ">Run: `{{ run_id }}` | {{ ts }}\n"
            ">Failed task: `{{ ti.xcom_pull(key='failed_task') }}`\n"
            ">Check Airflow logs immediately."
        ),
        channel="#data-alerts",
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # ── DAG dependencies ───────────────────────────────────────────────────────
    data_quality_check >> dbt_run >> dbt_test >> freshness_check
    freshness_check >> [slack_success, freshness_sla_breach]
    [data_quality_check, dbt_run, dbt_test] >> slack_failure
