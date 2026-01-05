# bronze_ingest.py
from config import get_spark, JDBC_URL, JDBC_PROPS, BRONZE_PATH, SQLSERVER_SCHEMA_SRC

TABLES = [
    "Customers", "Products", "Stores", "Orders", "OrderLines"
]

def main():
    spark = get_spark("Bronze_Ingest")

    for t in TABLES:
        src_table = f"{SQLSERVER_SCHEMA_SRC}.{t}"
        df = spark.read.jdbc(url=JDBC_URL, table=src_table, properties=JDBC_PROPS)

        # Optional: add ingestion metadata
        df = df.withColumn("_ingest_ts", df.sparkSession.sql("select current_timestamp()").collect()[0][0])

        df.write.mode("overwrite").parquet(f"{BRONZE_PATH}/{t.lower()}")
        print(f"Wrote Bronze: {t}")

    spark.stop()

if __name__ == "__main__":
    main()
