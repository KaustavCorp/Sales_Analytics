# dags/sales_analytics.py
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Go up two levels from current file to opt\airflow level
project_root = os.path.dirname(os.path.dirname(__file__))
# Base paths for your PySpark apps
BRONZE_APP = os.path.join(project_root,"jobs","bronze_ingest.py")
SILVER_APP = os.path.join(project_root,"jobs","silver_transform.py")
GOLD_APP = os.path.join(project_root,"jobs","gold_star.py")
PUBLISH_APP = os.path.join(project_root,"jobs","publish_sqlserver.py")

default_args = {
    "owner": "Kaustav_Das_Data_Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="sales_analytics",
    description="Bronze → Silver → Gold → Publish (sql Server) for sales analytics",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["sales", "analytics", "pyspark"]
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
    )

    silver_transform = SparkSubmitOperator(
        task_id="silver_transformation",
        application=SILVER_APP,
        name="sales_silver_transform",
        conn_id="spark_default",
        verbose=True
    )

    gold_star = SparkSubmitOperator(
        task_id="gold_star",
        application=GOLD_APP,
        name="sales_gold_star",
        conn_id="spark_default",
        verbose=True
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
    )

    # Dependencies: Bronze → Silver → Gold → Publish
    bronze_ingest >> silver_transform >> gold_star >> publish_sqlserver