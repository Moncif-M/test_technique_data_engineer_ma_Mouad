from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

params = {"start": "2023-01-01", "end": "2023-03-31"}
default_args = {"retries": 1, "retry_delay": timedelta(minutes=1)}

def make_task(task_id: str, script_name: str, args=""):
    return BashOperator(
        task_id=task_id,
        bash_command=("cd /opt/airflow/spark && python jobs/" + script_name + " " + args).strip()
    )


with DAG(dag_id="taxi_pipeline", start_date=datetime(2023, 1, 1), schedule=None, catchup=False, params=params, default_args=default_args, max_active_runs=1, 
         is_paused_upon_creation=False) as dag: 

    bronze_task = make_task("source_to_bronze", "source_to_bronze_trips.py", "--start {{ params.start }} --end {{ params.end }}")

    silver_task = make_task("bronze_to_silver", "bronze_to_silver_trips.py", "--start {{ params.start }} --end {{ params.end }}")

    gold_aggregate_task = make_task("gold_aggregate", "gold_aggregate_trips.py")

    gold_to_postgres_task = make_task("gold_to_postgres", "gold_to_postgres_trips.py")

    bronze_task >> silver_task >> gold_aggregate_task >> gold_to_postgres_task


