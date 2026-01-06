# publish_sqlserver.py
from config import get_spark, JDBC_URL, JDBC_PROPS, GOLD_PATH, SQLSERVER_SCHEMA_DW

TABLE_MAP = {
    f"{SQLSERVER_SCHEMA_DW}.DimCustomer": f"{GOLD_PATH}/dim_customer",
    f"{SQLSERVER_SCHEMA_DW}.DimProduct":  f"{GOLD_PATH}/dim_product",
    f"{SQLSERVER_SCHEMA_DW}.DimStore":    f"{GOLD_PATH}/dim_store",
    f"{SQLSERVER_SCHEMA_DW}.DimDate":     f"{GOLD_PATH}/dim_date",
    f"{SQLSERVER_SCHEMA_DW}.FactSales":   f"{GOLD_PATH}/fact_sales"
}

def main():
    spark = get_spark("Publish_SQLServer")
    for dbtable, path in TABLE_MAP.items():
        df = spark.read.parquet(path)
        # Drop helper columns if present
        if "year" in df.columns:
            df = df.drop("year")

        # Overwrite DW tables
        df.write \
          .format("jdbc") \
          .mode("append") \
          .option("url", JDBC_URL) \
          .option("dbtable", dbtable) \
          .option("user", JDBC_PROPS["user"]) \
          .option("password", JDBC_PROPS["password"]) \
          .option("driver", JDBC_PROPS["driver"]) \
          .save()
        print(f"Published {dbtable}")



if __name__ == "__main__":
    main()