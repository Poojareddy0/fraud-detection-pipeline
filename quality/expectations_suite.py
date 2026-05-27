"""
Great Expectations data quality suite for the fraud detection pipeline.
Runs inline before Silver layer load — bad batches are quarantined, not loaded.

Usage:
    python expectations_suite.py --table bronze.transactions --env dev
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

SYNAPSE_CONN = os.environ["SYNAPSE_CONN_STR"]


def build_context() -> gx.DataContext:
    """Build an in-memory GE context backed by Synapse."""
    context = gx.get_context()

    datasource_config = {
        "name": "synapse_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": SYNAPSE_CONN,
        },
        "data_connectors": {
            "runtime_connector": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["run_id"],
            }
        },
    }
    context.add_datasource(**datasource_config)
    return context


def build_transaction_suite(context: gx.DataContext) -> str:
    """Define expectations for the transactions Bronze table. Returns suite name."""
    suite_name = "transactions.bronze.suite"

    try:
        suite = context.get_expectation_suite(suite_name)
        logger.info("Loaded existing suite: %s", suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)
        logger.info("Created new suite: %s", suite_name)

    # ── Completeness ──────────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "transaction_id"},
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "customer_id"},
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "amount"},
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={"column": "transaction_ts"},
    ))

    # ── Uniqueness ────────────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "transaction_id"},
    ))

    # ── Domain validity ───────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "amount", "min_value": 0.01, "max_value": 1_000_000},
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "merchant_category",
            "value_set": ["GROCERY", "ELECTRONICS", "TRAVEL", "DINING",
                          "GAS", "ONLINE", "ATM", "OTHER"],
        },
    ))
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_value_lengths_to_equal",
        kwargs={"column": "merchant_country", "value": 2},
    ))

    # ── Fraud score range ─────────────────────────────────────────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "fraud_score", "min_value": 0, "max_value": 100},
    ))

    # ── Freshness: no records older than 2 hours in a batch ───────────────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "ingested_at",
            "min_value": {"$PARAMETER": "now - 2 hours"},
        },
    ))

    # ── Volume: at least 100 rows per batch (catches empty-load bugs) ─────────
    suite.add_expectation(gx.core.ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 100},
    ))

    context.save_expectation_suite(suite)
    return suite_name


def run_checkpoint(context: gx.DataContext, suite_name: str, run_id: str) -> bool:
    """
    Run a GE checkpoint against the latest Bronze batch.
    Returns True if all expectations pass, False otherwise.
    """
    batch_request = RuntimeBatchRequest(
        datasource_name="synapse_datasource",
        data_connector_name="runtime_connector",
        data_asset_name="transactions",
        runtime_parameters={"query": "SELECT * FROM bronze.transactions WHERE ingested_at >= DATEADD(minute, -15, GETUTCDATE())"},
        batch_identifiers={"run_id": run_id},
    )

    checkpoint_config = {
        "name": "transactions_checkpoint",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": f"fraud-pipeline-%Y%m%d-%H%M%S-{run_id}",
        "validations": [
            {
                "batch_request": batch_request,
                "expectation_suite_name": suite_name,
            }
        ],
    }

    context.add_or_update_checkpoint(**checkpoint_config)
    result = context.run_checkpoint(checkpoint_name="transactions_checkpoint")

    success = result.success
    stats = result.run_results

    for key, val in stats.items():
        r = val["validation_result"]
        logger.info(
            "Suite: %s | success=%s | passed=%d/%d",
            suite_name,
            r.success,
            r.statistics["successful_expectations"],
            r.statistics["evaluated_expectations"],
        )

    return success


def main() -> None:
    parser = argparse.ArgumentParser(description="Run GE data quality suite")
    parser.add_argument("--run-id", default="manual", help="Run identifier")
    parser.add_argument("--fail-on-error", action="store_true", default=True)
    args = parser.parse_args()

    context = build_context()
    suite_name = build_transaction_suite(context)
    passed = run_checkpoint(context, suite_name, args.run_id)

    if not passed:
        logger.error("Data quality checks FAILED for run_id=%s. Blocking Silver load.", args.run_id)
        if args.fail_on_error:
            sys.exit(1)
    else:
        logger.info("All data quality checks passed for run_id=%s.", args.run_id)


if __name__ == "__main__":
    main()
