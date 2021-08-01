import os
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import timedelta


project_dir = os.path.join(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))), "dags")

args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'email': ['ilyasrizky637@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id="end_to_end_pipeline",
    default_args=args,
    description="pipeline etl from postgres to hadoop hdfs",
    schedule_interval="0 2 * * *",
    start_date=days_ago(2),
    tags=['pipelining']
)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/usr/local/spark/pipeline/etl_script.py",
    name="Batch ETL with Spark",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master": "spark://spark:7077"},
    jars="/usr/local/spark/connectors/postgresql-9.4.1207.jar",
    driver_class_path="/usr/local/spark/connectors/postgresql-9.4.1207.jar",
    dag=dag
)
