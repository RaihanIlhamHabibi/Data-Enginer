"""Daily orders ETL DAG.

The staging directory must be a persistent volume shared by all Airflow workers.
The customer file and database credentials are configuration, not source code.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from pathlib import Path

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

STAGING_DIR = Path(os.environ["ORDERS_STAGING_DIR"])
CUSTOMERS_FILE = Path(os.environ["CUSTOMERS_FILE"])
POSTGRES_CONN_ID = os.environ.get("ORDERS_POSTGRES_CONN_ID", "orders_postgres")


def _safe_run_id(run_id: str) -> str:
    """Make an Airflow run id safe to use as a filename component."""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id)


def _write_csv_atomically(dataframe: pd.DataFrame, destination: Path) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    dataframe.to_csv(temporary, index=False)
    temporary.replace(destination)
    return str(destination)


def fetch_orders() -> pd.DataFrame:
    """Return a representative mock response for the source API.

    Replace this fixture with the authenticated API client used in production.
    The production client must return the same required columns.
    """
    orders = [
        {"order_id": "ORD-001", "customer_id": "C100", "amount": 100.0},
        {"order_id": "ORD-002", "customer_id": "C101", "amount": None},
        {"order_id": "ORD-001", "customer_id": "C100", "amount": 100.0},
    ]
    return pd.DataFrame(orders)


@dag(
    dag_id="daily_orders_etl",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["orders", "etl"],
)
def daily_orders_etl():
    @task
    def extract() -> str:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        orders = fetch_orders()
        required = {"order_id", "customer_id", "amount"}
        missing = required.difference(orders.columns)
        if missing:
            raise ValueError(f"Orders response is missing columns: {sorted(missing)}")

        run_id = _safe_run_id(context["run_id"])
        return _write_csv_atomically(orders, STAGING_DIR / f"orders-{run_id}.csv")

    @task
    def transform(orders_path: str) -> str:
        if not CUSTOMERS_FILE.is_file():
            raise FileNotFoundError(
                f"Customer file does not exist: {CUSTOMERS_FILE}. "
                "Provision it before the DAG runs or configure CUSTOMERS_FILE."
            )

        orders = pd.read_csv(orders_path)
        customers = pd.read_csv(CUSTOMERS_FILE)
        required_customer_columns = {"customer_id"}
        missing = required_customer_columns.difference(customers.columns)
        if missing:
            raise ValueError(f"Customer file is missing columns: {sorted(missing)}")
        if customers["customer_id"].duplicated().any():
            raise ValueError("Customer file contains duplicate customer_id values")

        orders["amount"] = pd.to_numeric(orders["amount"], errors="coerce")
        invalid_amounts = orders["amount"].isna().sum()
        if invalid_amounts:
            logger.warning("Dropping %d orders with null or invalid amount", invalid_amounts)
            orders = orders.dropna(subset=["amount"])

        orders = orders.drop_duplicates(subset=["order_id"], keep="last")
        result = orders.merge(customers, on="customer_id", how="inner", validate="many_to_one")
        if len(result) != len(orders):
            logger.warning("Dropped %d orders without a matching customer", len(orders) - len(result))

        return _write_csv_atomically(
            result,
            Path(orders_path).with_name(Path(orders_path).stem + "-transformed.csv"),
        )

    @task
    def load(transformed_path: str) -> None:
        dataframe = pd.read_csv(transformed_path)
        if dataframe.empty:
            logger.info("No valid orders to load")
            return

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        try:
            with engine.begin() as connection:
                dataframe.to_sql(
                    "orders",
                    connection,
                    if_exists="append",
                    index=False,
                    chunksize=1000,
                    method="multi",
                )
        finally:
            engine.dispose()

    extracted_path = extract()
    transformed_path = transform(extracted_path)
    load(transformed_path)


dag = daily_orders_etl()
