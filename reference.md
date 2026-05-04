# 🚀 AIRFLOW MASTER GUIDE (ULTIMATE – PRODUCTION + INTERVIEW DEPTH)

---

# 1. INTRODUCTION

Apache Airflow is a workflow orchestration system used to define, schedule, and monitor data pipelines.
It operates as a control plane, meaning it coordinates tasks but does not execute heavy computation itself.
In production systems, Airflow integrates with tools like Databricks, Snowflake, and S3 to orchestrate end-to-end pipelines.
Understanding this separation is critical for designing scalable and maintainable data platforms.

---

# 2. AIRFLOW ARCHITECTURE (INTERNALS)

## Scheduler
The scheduler continuously parses DAG files, creates DAG runs, and determines which tasks are ready to execute.
It evaluates dependencies and pushes runnable tasks to the executor.
This loop runs every few seconds, making it the brain of Airflow.

Example:
If task A completes successfully, the scheduler evaluates downstream tasks and queues task B.

---

## Metadata Database
This is the backbone of Airflow where all states are stored, including DAG runs, task instances, logs, and XCom data.
Every decision in Airflow is driven by metadata stored here, so its health is critical.
In production, this is typically PostgreSQL or MySQL.

Example SQL:
SELECT * FROM task_instance WHERE dag_id = 'example_dag';

---

## Executor
The executor determines how tasks are executed.
It takes tasks from the scheduler and assigns them to workers or containers.

Example:
executor = CeleryExecutor

---

## Workers
Workers execute tasks using operators.
They fetch tasks from queues and execute the actual logic defined in operators.

---

# 3. DAG (DEEP DIVE)

A DAG (Directed Acyclic Graph) defines the structure of your workflow.
It ensures tasks run in a specific order without cycles.

---

## DAG Example

```python
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="example_dag",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
):
    pass
```

---

## DAG CONFIGURATIONS

### start_date
Defines the logical start time of the DAG.
Airflow uses this to compute execution dates, not actual runtime.

Example:
start_date=datetime(2024,1,1)

---

### schedule_interval
Defines how often the DAG runs using cron or presets.
This directly impacts data freshness and pipeline timing.

Example:
schedule_interval="0 2 * * *"

---

### catchup
Controls whether historical runs should execute.
In production, usually set to False to avoid backfill overload.

Example:
catchup=False

---

### max_active_runs
Limits how many DAG runs can execute simultaneously.
Prevents duplicate processing and race conditions.

Example:
max_active_runs=1

---

# 4. TASKS

Tasks are the smallest units of execution in Airflow.
Each task should be independent, idempotent, and retry-safe.

---

## Task Example

```python
from airflow.operators.python import PythonOperator

def process():
    print("processing")

task = PythonOperator(
    task_id="task1",
    python_callable=process
)
```

---

# 5. PRODUCTION GRADE DAG (FULL PIPELINE)

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="prod_pipeline",
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    list_files = S3ListOperator(
        task_id="list_files",
        bucket="raw_bucket",
        prefix="incoming/"
    )

    databricks_job = DatabricksSubmitRunOperator(
        task_id="dbx_job",
        databricks_conn_id="databricks_default",
        json={
            "new_cluster": {
                "spark_version": "13.x",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            },
            "notebook_task": {
                "notebook_path": "/Repos/etl"
            }
        }
    )

    snowflake_load = SnowflakeOperator(
        task_id="sf_load",
        sql="COPY INTO table FROM @stage"
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_next",
        trigger_dag_id="downstream_dag"
    )

    list_files >> databricks_job >> snowflake_load >> trigger
```

---

# 6. XCOM

XCom is used to pass small amounts of data between tasks.
It stores data in the metadata database as serialized JSON.
It should not be used for large datasets, as it impacts performance.

---

## Example

```python
def push(**context):
    context['ti'].xcom_push(key='count', value=10)

def pull(**context):
    val = context['ti'].xcom_pull(task_ids='push', key='count')
```

---

# 7. S3 OPERATORS

S3 operators allow interaction with AWS S3 for listing, uploading, or sensing files.
They are commonly used in ingestion pipelines.

---

## Example

```python
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

task = S3ListOperator(
    task_id="list_files",
    bucket="my-bucket",
    prefix="data/"
)
```

---

# 8. DATABRICKS OPERATOR

This operator submits jobs to Databricks clusters.
It supports both new cluster creation and existing cluster usage.

---

## Example

```python
DatabricksSubmitRunOperator(
    task_id="dbx",
    databricks_conn_id="databricks_default",
    json={"new_cluster": {}, "notebook_task": {}}
)
```

---

# 9. SNOWFLAKE OPERATOR

Used to execute SQL queries on Snowflake.
Common use cases include loading data and performing transformations.

---

## Example

```python
SnowflakeOperator(
    task_id="load",
    sql="COPY INTO table FROM @stage"
)
```

---

# 10. DYNAMIC TASKS

Dynamic task mapping allows runtime task generation based on input data.
This is useful for processing multiple files or partitions.

---

## Example

```python
from airflow.decorators import task

@task
def process(file):
    print(file)

process.expand(file=["a","b","c"])
```

---

# 11. TASK DEPENDENCIES

Dependencies define execution order between tasks.
They ensure correct sequencing in pipelines.

---

## Example

```python
task1 >> task2
```

---

# 12. DAG DEPENDENCIES

DAGs can depend on each other using triggers or sensors.

---

## Trigger Example

```python
TriggerDagRunOperator(
    task_id="trigger",
    trigger_dag_id="next_dag"
)
```

---

## Sensor Example

```python
ExternalTaskSensor(
    task_id="wait",
    external_dag_id="dag1",
    external_task_id="task1"
)
```

---

# 13. SENSORS

Sensors wait for external conditions like file arrival.
They can block resources if not used properly.

---

## Example

```python
S3KeySensor(
    task_id="wait",
    bucket_key="file.csv",
    bucket_name="bucket"
)
```

---

# 14. CLI COMMANDS

These commands help in debugging and managing DAGs.

---

## Example

```bash
airflow dags list
airflow dags trigger my_dag
airflow tasks test my_dag task1 2024-01-01
```

---

# 15. BEST PRACTICES

- Keep DAGs lightweight
- Use external systems for compute
- Ensure idempotency
- Monitor failures properly

---

# END
