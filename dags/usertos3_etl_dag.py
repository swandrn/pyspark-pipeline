from etl import extract
from etl import transform
from etl import load
from etl import paths
from etl import config
from collections import Counter
import logging
import threading
from queue import Queue
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

def repeatedly_call_users(iter: int) -> list:
    q = Queue()
    tasks = []
    users_json = []
    errors = []

    for _ in range(iter):
        tasks.append(threading.Thread(target=extract.call_random_user, args=[f'https://randomuser.me/api/1.4/?exc=id&results={config.RESULTS}&nat=gb', q]))

    for task in tasks:
        task.start()
        res = q.get()
        if isinstance(res, list):
            users_json = users_json + res
        else:
            errors.append(res)

    for task in tasks:
        task.join()

    if errors:
        unique_errors = Counter(errors)
        for err in unique_errors:
            logging.info(f'{err} has {unique_errors[err]} occurences')
            
    return users_json

with DAG(
    dag_id='Userme-S3-pipeline',
    render_template_as_native_obj=True,
) as dag:
    
    print_path = BashOperator(
        task_id='print_path',
        bash_command="""
            airflow info;
        """,
    )

    start_etl = EmptyOperator(
        task_id='start_etl',
    )

    fetch_users = PythonOperator(
        task_id='fetch_users',
        provide_context=True,
        do_xcom_push=True,
        python_callable=repeatedly_call_users,
        op_kwargs={
            'iter': config.API_CALL
        },
    )

    convert_to_df = PythonOperator(
        task_id='convert_to_df',
        provide_context=True,
        do_xcom_push=True,
        python_callable=transform.user_json_to_df,
        op_args=["{{task_instance.xcom_pull(task_ids='fetch_users')}}"],
    )

    write_to_s3 = PythonOperator(
        task_id='write_to_s3',
        python_callable=load.pandas_df_to_s3,
        op_kwargs={
            'df': '{{task_instance.xcom_pull(task_ids="convert_to_df")}}',
            'bucket': paths.CC_BUCKET,
            'key': 'test_destination/users.csv',
        },
    )

    end_etl = EmptyOperator(
        task_id='end_etl',
    )

print_path >> start_etl >> fetch_users >> convert_to_df >> write_to_s3 >> end_etl