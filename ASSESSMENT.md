# Data Engineer Assessment: Daily Orders ETL

## 1. Failure diagnosis

**Root cause:** the `transform` task fails at `pd.read_csv('/data/customers.csv')` because that exact path does not exist in the Airflow worker environment. The original code has no validation or fallback for a missing customer file, so the `FileNotFoundError` is unhandled. The log identifies this as the first exception. The load task is correctly skipped because its upstream task failed.

There is also a separate execution defect in the original DAG: `transform(orders_df)` requires an argument, but the `PythonOperator` does not pass the output of `extract`. Unless the callable has a default or `op_kwargs` is configured, transform would fail with a missing-argument `TypeError` after the file issue was fixed.

## 2. Other code issues

- `import pandas as pd from sqlalchemy import create_engine` is invalid Python syntax; the imports must be separate.
- `retries=0` means a transient API, file-system, or database error causes immediate failure with no automatic recovery.
- There is no explicit error handling or actionable validation around the customer file, source schema, customer schema, API response, or database load.
- The customer path and database credentials are hard-coded, which prevents deployment across environments and risks credential exposure.
- The DAG has no timeout, catchup policy, concurrency protection, or task-level logging/metrics beyond default Airflow behavior.
- The naive task graph would place the complete orders DataFrame in XCom. XCom is metadata storage, not a bulk-data transport, and 42,156 rows can cause database growth and serialization problems.
- The sample data includes a duplicate `order_id` and a null amount. Loading both rows can duplicate business facts or violate target constraints.
- The merge does not validate customer-key uniqueness, does not specify join semantics, and silently loses orders without a matching customer.
- `to_sql(..., if_exists='append')` is not idempotent. A retry after a partially successful load can insert duplicates unless the target has a uniqueness constraint and the load uses an upsert or staging-and-merge pattern.
- There is no schema validation, transaction boundary, batching, cleanup, or protection against partially written staging files.
- A naive `datetime` start date is timezone-ambiguous, and the old `schedule_interval` style is less clear on current Airflow versions.

## 3. Production impact

The immediate impact is a failed daily run and a downstream pipeline/reporting delay or downtime because the load task is skipped. With an inner join, orders whose customer is missing are silently discarded, causing data loss without an alert. Duplicate order rows can inflate revenue or violate target constraints, while null amounts can create invalid financial records. With `retries=0`, transient failures remain unresolved until manual intervention. Hard-coded credentials can cause a security incident, and hard-coded local paths fail when tasks run on another worker or container. Large XCom payloads can exhaust the metadata database or worker memory. Retrying a partial append can create duplicate loads unless the target is idempotent.

## Remediated script

The complete implementation is in [transform_orders.py](transform_orders.py). Key changes are:

1. Fixed imports and modernized the DAG definition with UTC scheduling, `catchup=False`, a run timeout, bounded active runs, and two retries with backoff.
2. Replaced hard-coded infrastructure values with `CUSTOMERS_FILE`, `ORDERS_STAGING_DIR`, and an Airflow Postgres connection id.
3. Writes extract and transform results to atomically replaced files on a shared persistent volume. Only paths, not order rows, move through XCom.
4. Validates source and customer schemas and fails with an actionable error when the customer file is unavailable.
5. Coerces amounts, drops invalid monetary records with a warning, deduplicates by `order_id`, and requires a many-to-one customer relationship.
6. Uses a database transaction, batches inserts, and disposes the SQLAlchemy engine.

### Change-by-change explanation

| Before | After | Why |
| --- | --- | --- |
| Invalid combined import statement | Separate valid imports | The DAG must parse and import successfully. |
| `PythonOperator` tasks did not pass the extracted DataFrame | TaskFlow dependencies pass staging-file paths between tasks | The transform receives its input, while bulk rows stay out of XCom. |
| `pd.read_csv('/data/customers.csv')` with no check | `CUSTOMERS_FILE` configuration plus an explicit `FileNotFoundError` message | Workers can use environment-specific paths and operators get an actionable failure. |
| Database URL contained `user:pass@localhost` | `PostgresHook` uses an Airflow Connection ID | Credentials stay in Airflow's connection/secrets backend and deployments are portable. |
| `retries=0` and no run limit | Two retries, five-minute delay, two-hour DAG timeout, `max_active_runs=1`, and `catchup=False` | Transient failures can recover without overlapping or unbounded runs. |
| No schema or data-quality checks | Required-column checks, numeric amount coercion, duplicate-key handling, and `many_to_one` merge validation | Bad inputs fail or are handled explicitly before loading business data. |
| Full DataFrames would be serialized through XCom | Atomic CSV staging on a shared persistent volume; only file paths use XCom | Reduces metadata-database growth and worker memory pressure for 42,156 rows. |
| Unbatched append with an unmanaged engine | Transaction, `chunksize=1000`, multi-row inserts, and engine disposal | Limits memory use and prevents partial writes within the database transaction. |

### Required deployment configuration

The `fetch_orders()` function contains a small, representative mock response only so the assessment code has a runnable source contract. It is not a production extractor. Before activation, replace it with the authenticated orders API client, including API timeout, pagination, and secret handling. The client must return `order_id`, `customer_id`, and `amount` columns.

Set these values in the Airflow worker/scheduler environment or deployment configuration; do not edit the DAG to add machine-specific paths or secrets:

```text
ORDERS_STAGING_DIR=/shared/airflow/orders
CUSTOMERS_FILE=/shared/airflow/reference/customers.csv
ORDERS_POSTGRES_CONN_ID=orders_postgres
```

For a Linux worker or Docker Compose `.env` file:

```bash
export ORDERS_STAGING_DIR=/shared/airflow/orders
export CUSTOMERS_FILE=/shared/airflow/reference/customers.csv
export ORDERS_POSTGRES_CONN_ID=orders_postgres
```

For PowerShell on a Windows test worker:

```powershell
$env:ORDERS_STAGING_DIR = 'C:\airflow\shared\orders'
$env:CUSTOMERS_FILE = 'C:\airflow\shared\reference\customers.csv'
$env:ORDERS_POSTGRES_CONN_ID = 'orders_postgres'
```

In Airflow, create the database connection through **Admin > Connections** with `Connection Id = orders_postgres`, type `Postgres`, and the host/database/user/password/TLS fields populated from the secrets backend. The three path/ID values above are process environment variables configured in Docker Compose, Helm/Kubernetes `env`, or the worker service definition. Airflow Variables do not automatically become `os.environ` values; use `Variable.get()` in the DAG only if the implementation is intentionally changed to read Variables instead.

`ORDERS_STAGING_DIR` must point to a persistent volume mounted at the same path on every worker running these tasks. `CUSTOMERS_FILE` must point to a file on that shared volume, refreshed by an upstream task or sensor with a freshness check. `ORDERS_POSTGRES_CONN_ID` is only the Airflow Connection ID; create that connection in Airflow and store the host, database, username, password, and TLS settings in the Airflow secrets backend or connection UI. No database credential is present in the DAG.

Also add a target uniqueness constraint and an idempotent upsert keyed by `order_id` (or by `order_id` and business date). The script intentionally does not invent the target table's customer columns or conflict policy.

## Bonus: resource efficiency on a 12 GB / 4-core server

- Reserve approximately 2.5 GB for the OS, Airflow scheduler/webserver, metadata database, and monitoring overhead, leaving about 8.5 GB for task processes. This leaves practical headroom instead of budgeting the full 12 GB.
- For this single 4-core/4-thread host, use `LocalExecutor` with an initial `parallelism = 3`, `max_active_tasks_per_dag = 2`, and this DAG's `max_active_runs = 1`. Start the orders DAG with a pool of 1 or 2 slots because its pandas join is memory-heavy.
- If `CeleryExecutor` is required, keep `worker_concurrency = 2` on this host and set the worker memory limit near 2 GB per process. Do not run four pandas workers concurrently until RSS measurements show that the remaining memory is safe. Celery is more useful when workers run on separate machines; `LocalExecutor` is simpler when all capacity is local.
- Set Airflow `parallelism = 3`, `max_active_tasks_per_dag = 2`, and separate pools for API and database work. This leaves one CPU slot of headroom for scheduler/webserver activity instead of saturating all four cores.
- Keep task payloads out of XCom. Use object storage, a shared volume, or database staging tables, and pass only URIs and run metadata.
- Process large CSV extracts in chunks of 1,000-5,000 rows, select only required columns, and load each chunk in matching batches. Avoid holding multiple full copies during joins; prefer database-side joins and incremental extraction where practical.
- Configure worker and scheduler memory limits, monitor RSS, task duration, swap, OOM kills, XCom size, database connections, and queue depth. Alert before the host reaches exhaustion; a practical initial alert is 80% host memory usage and 90% worker memory usage.
- Use connection pools and bounded retries with exponential backoff so a database or API outage does not create a retry storm. Apply timeouts to HTTP calls and database statements.
- Keep the metadata database on a supported external service if possible; otherwise schedule regular cleanup of task logs, XCom rows, and old DAG runs, with backups.
- Ensure every task closes HTTP, file, and database resources. Prefer short-lived task processes for suspected leaks and restart workers between batches if profiling confirms a library leak.
- Add data-quality checks for row counts, null rates, duplicate keys, referential coverage, and freshness. Test with representative volumes before increasing concurrency.

## Safeguards before activation

The customer-file refresh should be an explicit upstream task or sensor with a freshness check, rather than an implicit local-file assumption. CI should run syntax, import, unit, and DAG-parsing tests. A staging load followed by an idempotent merge, data-quality assertions, and alerting on task failure, retries, SLA/freshness breach, and abnormal row counts should be required before production scheduling.

## Submission checklist

Attach both files to the email:

- [`ASSESSMENT.md`](ASSESSMENT.md), containing the written answers and remediation explanation.
- [`transform_orders.py`](transform_orders.py), containing the remediated DAG.

Use this exact subject format, replacing the placeholder:

```text
Data Engineer - [Nama Kamu]
```

Suggested short email body:

```text
Dear Nodewave team,

Please find attached my submission for the Data Engineer assessment:
1. ASSESSMENT.md - diagnosis, remediation explanation, and resource recommendations.
2. transform_orders.py - remediated Apache Airflow ETL DAG.

The same files are also available at the test link submitted in the form.

Regards,
[Nama Kamu]
```

The Tally form at https://tally.so/r/GxOyOO does not show a file-upload field. It asks for Name, Email, WhatsApp, Link CV/Resume, and Link Test. Upload the two files to an accessible location (for example, a private share link permitted by the recruiter), put that URL in **Link Test**, and still attach both files to the email sent to `rigen@nodewave.id` and `rafizhafran.halim@nodewave.id`.