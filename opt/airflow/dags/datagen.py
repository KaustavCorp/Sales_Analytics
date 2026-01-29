from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Go up two levels from current file to opt\airflow level
project_root = os.path.dirname(os.path.dirname(__file__))
# Base path for your Python app
DATAGEN_APP = os.path.join(project_root,"utils","DataGenerator","SalesDataFaker.py")
# --- Define DAG ---
default_args = {
    "owner": "Kaustav_Das_Data_Engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False
}

with DAG(
    dag_id="datagen",
    description="Generate synthetic sales data using Faker",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["sales", "faker", "data-gen","python-script"],
) as dag:

    generate_data_task = BashOperator(
        task_id="generate_sales_data",
        bash_command=f"python {DATAGEN_APP}"  # <-- path to your script
    )

    generate_data_task
