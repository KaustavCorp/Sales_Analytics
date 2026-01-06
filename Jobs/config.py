# config.py
import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
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

# --- Storage paths (adjust to ADLS/S3/local FS) ---
BRONZE_PATH = os.getenv("BRONZE_PATH", "data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "data/silver")
GOLD_PATH   = os.getenv("GOLD_PATH",   "data/gold")

# --- Spark Config ---

JDBC_JAR = os.getenv("JDBC_JAR", "file:///D:/Demo/Sales_Analytics/jars/mssql-jdbc-13.2.1.jre11.jar")

def get_spark(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", JDBC_JAR)  # ensure JDBC driver is loaded
        .getOrCreate()
    )
