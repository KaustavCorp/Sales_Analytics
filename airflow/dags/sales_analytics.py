# dags/sales_analytics.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Base paths for your PySpark apps
Jobs_Path = '/opt/airflow/jobs/'
BRONZE_APP = f"{Jobs_Path}bronze_ingest.py"
SILVER_APP = f"{Jobs_Path}silver_transform.py"
GOLD_APP   = f"{Jobs_Path}gold_star.py"
PUBLISH_APP= f"{Jobs_Path}publish_sqlserver.py"


default_args = {
    "owner": "Kaustav_Das_Data_Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="sales_analytics",
    description="Bronze → Silver → Gold → Publish (sql Server) for sales analytics",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["sales", "analytics", "pyspark"],
) as dag:

    bronze_ingest = SparkSubmitOperator(
        task_id="bronze_ingest",
        application=BRONZE_APP,
        name="sales_bronze_ingest",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.jars.packages": "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"
        }
        #conf={"spark.master": "local[*]"},
    )

    silver_transform = SparkSubmitOperator(
        task_id="silver_transformation",
        application=SILVER_APP,
        name="sales_silver_transform",
        conn_id="spark_default",
        verbose=True,
        #conf={"spark.master": "local[*]"},
    )

    gold_star = SparkSubmitOperator(
        task_id="gold_star",
        application=GOLD_APP,
        name="sales_gold_star",
        conn_id="spark_default",
        verbose=True,
        #conf={"spark.master": "local[*]"},
    )

    publish_sqlserver = SparkSubmitOperator(
        task_id="publish_sqlserver",
        application=PUBLISH_APP,
        name="sales_publish_sqlserver",
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.jars.packages": "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11"
        }
        #conf={"spark.master": "local[*]"},
    )

    # Dependencies: Bronze → Silver → Gold → Publish
    bronze_ingest >> silver_transform >> gold_star >> publish_sqlserver
