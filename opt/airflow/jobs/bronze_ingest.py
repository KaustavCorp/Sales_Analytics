from config import get_spark, JDBC_URL, JDBC_PROPS, BRONZE_PATH, SQLSERVER_SCHEMA_SRC
from pyspark.sql.functions import current_timestamp

TABLES = ["Customers", "Products", "Stores", "Orders", "OrderLines"]

def main():
    spark = get_spark("Bronze_Ingest")

    for t in TABLES:
        src_table = f"{SQLSERVER_SCHEMA_SRC}.{t}"
        df = spark.read.jdbc(url=JDBC_URL, table=src_table, properties=JDBC_PROPS)

        # Add ingestion metadata
        df = df.withColumn("_ingest_ts", current_timestamp())

        # Ensure BRONZE_PATH uses file:// scheme
        df.write.mode("overwrite").parquet(f"{BRONZE_PATH}/{t.lower()}")
        print(f"Wrote Bronze: {t}")



if __name__ == "__main__":
    main()