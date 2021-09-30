import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


def _say_hello():
    logging.info("Hello, SAKSIAM")

def _say_error():
    logging.error('Hey Error')


default_args = {
    "owner": "Veerawat",
    "start_date": timezone.datetime(2021, 9, 30)
}
with DAG(
    "dag_homework_2",
    schedule_interval="*/30 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:

    start = DummyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo 'hello'",
    )

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    say_error = PythonOperator(
        task_id="say_error",
        python_callable=_say_error,
    )

    end = DummyOperator(task_id="end")

    start >> echo_hello >> say_hello >> say_error >> end
