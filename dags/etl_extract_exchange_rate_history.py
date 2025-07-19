from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator 
from airflow.utils.email import send_email
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os


load_dotenv()

def notify_email(context):
    distribution_list = Variable.get("EMAIL_DEBUG_LIST")
    if distribution_list == None:
        raise EnvironmentError('EMAIL_DEBUG_LIST value is not defined in environment file')

    subject = f"Airflow Failure: {context['task_instance'].task_id}"
    body = \
    f"""
    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Execution Time: {context['execution_date']}
    Log URL: {context['task_instance'].log_url}
    """
    send_email(distribution_list,subject,body)

@dag(
    dag_id="etl_extract_exchange_rate_history",
    description="ETL extract process for updating dashboards for exchange rate history.",
    tags=["ETL","Extract","Exchange_rates"],
    schedule=timedelta(days=1),
    start_date=datetime(2025,7,16),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback":notify_email,
        "depends_on_past": False,
    }
)
def etl_extract_exchange_rate_history():

    scripts_dir = Variable.get("SCRIPTS_DIR")
    logs_scripts_dir = Variable.get("LOGS_SCRIPTS_DIR")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")

    if scripts_dir == None:
        raise EnvironmentError('SCRIPTS_DIR value is not defined in environment file')
    
    if logs_scripts_dir == None:
        raise EnvironmentError('LOGS_AIRFLOW_DIR value is not defined in environment file')

    @task.bash
    def pull_exchange_rates_api_json_files_to_local():
        return f'python {scripts_dir}/extract.py >> {logs_scripts_dir}/extract_{timestamp}.log 2>&1'
    
    @task.bash
    def load_exchange_rates_files_to_snowflake():
        return f'python {scripts_dir}/db_load.py >> {logs_scripts_dir}/db_load_{timestamp}.log 2>&1'
    
    @task.bash
    def exchange_rates_files_cleanup():
        return f'sh {scripts_dir}/extract_cleanup.sh >> {logs_scripts_dir}/extract_cleanup_{timestamp}.log 2>&1'
    
    pull_exchange_rates_api_json_files_to_local() >> load_exchange_rates_files_to_snowflake() >> exchange_rates_files_cleanup()

dag = etl_extract_exchange_rate_history()