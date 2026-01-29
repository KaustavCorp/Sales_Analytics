# config.py
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
# Go up two level from current file to opt\airflow level
project_root = os.path.dirname(os.path.dirname(__file__))

# Point to env/.env inside project root
env_path = os.path.join(project_root, "env", ".env")
load_dotenv(dotenv_path=env_path)

# --- JDBC ---
SQLSERVER_HOST = os.getenv("SQLSERVER_HOST", "<server>")
SQLSERVER_PORT = os.getenv("SQLSERVER_PORT", "1433")
SQLSERVER_DB   = os.getenv("SQLSERVER_DB", "<db_name>")
SQLSERVER_USER = os.getenv("SQLSERVER_USER", "<user>")
SQLSERVER_PASS = os.getenv("SQLSERVER_PASS", "<password>")
SQLSERVER_SCHEMA_SRC = "src"   # source schema
SQLSERVER_SCHEMA_DW  = "dw"    # target DW schema

JDBC_URL = (
    f"jdbc:sqlserver://{SQLSERVER_HOST}:{SQLSERVER_PORT};"
    f"databaseName={SQLSERVER_DB};encrypt=true;trustServerCertificate=true"
)
JDBC_PROPS = {
    "user": SQLSERVER_USER,
    "password": SQLSERVER_PASS,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}


# --- Fetch storage paths from .env ---
RELATIVE_BRONZE_PATH = os.getenv("BRONZE_PATH")
BRONZE_PATH = os.path.join(project_root, RELATIVE_BRONZE_PATH)
RELATIVE_SILVER_PATH = os.getenv("SILVER_PATH")
SILVER_PATH = os.path.join(project_root, RELATIVE_SILVER_PATH)
RELATIVE_GOLD_PATH = os.getenv("GOLD_PATH")
GOLD_PATH = os.path.join(project_root, RELATIVE_GOLD_PATH)

# --- Spark Config ---
# Instead of a local JAR, use Maven coordinates for the sql Server JDBC driver
# Example: "com.microsoft.sqlserver:mssql-jdbc:12.2.0.jre11"
JDBC_PACKAGE = os.getenv("JDBC_PACKAGE", "com.microsoft.sqlserver:mssql-jdbc:13.2.1.jre11")

def get_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", JDBC_PACKAGE)  # use Maven package instead of local JAR
        .getOrCreate()
    )


