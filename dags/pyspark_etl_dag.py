from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

spark_conf = {
    'spark.jars.packages': 'org.apache.hadoop:hadoop-aws:3.3.6,org.apache.hadoop:hadoop-common:3.3.6,com.amazonaws:aws-java-sdk-bundle:1.12.367',
}

with DAG(
    dag_id='PySpark-ETL-pipeline',
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
    
    etl_pipeline = SparkSubmitOperator(
        task_id='etl_pipeline',
        name='credit_cards',
        application='./dags/pyspark_etl_main.py',
        conn_id='spark_default',
        conf=spark_conf,
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False,
    )

    end_etl = EmptyOperator(
        task_id='end_etl',
    )

print_path >> start_etl >> etl_pipeline >> end_etl