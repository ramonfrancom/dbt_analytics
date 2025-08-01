from airflow import DAG
from datetime import datetime
from lib.operators.control_table_opeartor import ControlTableOperator

with DAG(
    "test_control_operator", 
    start_date=datetime(2025, 1, 1), 
    schedule=None, 
    catchup=False
    ) as dag:

    test_task = ControlTableOperator(
        task_id="test_fetch",
        snowflake_conn_id="snowflake_conn",
        process_name="Historical Exchange Data Reports for USD",
        pulled_status=["Requested", "Completed"]
    )