from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello from Airflow'",
    )
