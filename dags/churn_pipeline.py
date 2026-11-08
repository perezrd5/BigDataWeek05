"""Airflow 2.10 DAG for a single-host lab with shared project storage."""
from datetime import datetime, timedelta, timezone
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG('churn_pipeline', schedule='@daily', start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), catchup=False, max_active_runs=1, default_args={'retries': 2, 'retry_delay': timedelta(minutes=2)}) as dag:
    root = os.getenv('LAB_PROJECT_ROOT', '/opt/etl-lab')
    python = os.getenv('LAB_PYTHON', root + '/.venv/bin/python')
    etl = BashOperator(task_id='etl', bash_command='"$LAB_PYTHON" -m pipeline.etl', cwd=root, env={'LAB_PYTHON': python}, append_env=True)
    train = BashOperator(task_id='train', bash_command='"$LAB_PYTHON" -m pipeline.train', cwd=root, env={'LAB_PYTHON': python}, append_env=True)
    etl >> train
