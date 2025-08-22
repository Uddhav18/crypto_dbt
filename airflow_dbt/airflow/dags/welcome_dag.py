from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def print_welcome():
    print('Welcome to Airflow!')

def print_date():
    print(f"Today is {datetime.today().date()}")

def print_random_quote():
    # ✅ keep requests inside the function
    response = requests.get('https://api.quotable.io/random', verify=False)  # workaround SSL
    quote = response.json()['content']
    print(f'Quote of the day: "{quote}"')

dag = DAG(
    dag_id="welcome_dag",
    default_args={'start_date': datetime(2025, 8, 20)},
    schedule="0 23 * * *",
    catchup=False
)

print_welcome_task = PythonOperator(
    task_id='print_welcome',
    python_callable=print_welcome,
    dag=dag
)

print_date_task = PythonOperator(
    task_id='print_date',
    python_callable=print_date,
    dag=dag
)

print_random_quote_task = PythonOperator(
    task_id='print_random_quote',
    python_callable=print_random_quote,
    dag=dag
)

# Set task dependencies
print_welcome_task >> print_date_task >> print_random_quote_task
