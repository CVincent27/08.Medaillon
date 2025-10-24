import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from scripts.generate_data import generate_data


with DAG(
    dag_id="dag_medaillon",
    start_date=datetime.datetime(2021, 1, 1)
) as dag:    
    start = EmptyOperator(task_id="start")
    generate = EmptyOperator(task_id="generate_data")

    start >> generate