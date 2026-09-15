# Data Engineer Assessment

Remediated Apache Airflow daily orders ETL pipeline for the Data Engineer assessment.

## Contents

- [ASSESSMENT.md](ASSESSMENT.md): failure diagnosis, production risks, remediation rationale, resource recommendations, and submission checklist.
- [transform_orders.py](transform_orders.py): the remediated Airflow DAG.

## Pipeline

The `daily_orders_etl` DAG runs daily at 02:00 UTC:

```text
extract -> transform -> load
```

The pipeline writes intermediate CSV files to a shared staging volume instead of putting full DataFrames into Airflow XCom. The transform step validates the customer file and source schema, removes invalid amounts, handles duplicate order IDs, and validates the customer join.

The `fetch_orders()` function is a representative mock fixture for the assessment. Replace it with the authenticated production API client before activation. The production client must return `order_id`, `customer_id`, and `amount` columns.

## Configuration

Set these environment variables for every Airflow worker that can run the DAG:

```bash
export ORDERS_STAGING_DIR=/shared/airflow/orders
export CUSTOMERS_FILE=/shared/airflow/reference/customers.csv
export ORDERS_POSTGRES_CONN_ID=orders_postgres
```

PowerShell example:

```powershell
$env:ORDERS_STAGING_DIR = 'C:\airflow\shared\orders'
$env:CUSTOMERS_FILE = 'C:\airflow\shared\reference\customers.csv'
$env:ORDERS_POSTGRES_CONN_ID = 'orders_postgres'
```

`ORDERS_STAGING_DIR` must be the same persistent-volume path on all workers. `CUSTOMERS_FILE` must exist on that shared volume and be refreshed by an upstream process or sensor. `ORDERS_POSTGRES_CONN_ID` is an Airflow Connection ID, not a password or connection string. Create `orders_postgres` under **Admin > Connections** and store database credentials in Airflow's secrets backend or connection configuration.

## Validation

Compile-check the Python module locally:

```bash
python -m py_compile transform_orders.py
```

A full DAG import test must run in an environment containing the Airflow package, the Postgres provider, `pandas`, `pendulum`, and the database driver. The local assessment folder does not bundle those runtime dependencies.

## Production prerequisites

Before scheduling this DAG in production:

1. Replace the mock extractor with the authenticated, paginated API client.
2. Provision the shared staging volume and customer-file refresh process.
3. Add a uniqueness constraint and idempotent upsert for the target order key.
4. Configure data-quality checks, alerts, and Airflow pools/concurrency limits.
5. Run representative-volume tests and verify the target schema and merge policy.

See [ASSESSMENT.md](ASSESSMENT.md) for the complete written submission.
