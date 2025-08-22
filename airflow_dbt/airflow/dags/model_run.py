from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="model_run_dag",
    default_args=default_args,
    schedule="0 12 * * *",   # ✅ correct in Airflow 2.7+
    catchup=False
) as dag:


    # Pull latest dbt code from git
    git_pull = BashOperator(
        task_id="git_pull",
       bash_command=""" rm -rf /opt/airflow/dags/dbt_project 
                        cd /opt/airflow/dags && git clone https://github.com/Uddhav18/crypto_dbt.git dbt_project
                        cd /opt/airflow/dags/dbt_project/crypto """

    )

    # Run dbt models
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dags/dbt_project && dbt run --profiles-dir .",
    )

    git_pull >> dbt_run
