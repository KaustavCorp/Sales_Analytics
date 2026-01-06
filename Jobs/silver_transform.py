# silver_transform.py
from pyspark.sql.functions import col, trim, upper, lower, when, to_date
from pyspark.sql.types import DecimalType, DateType, IntegerType, StringType
from config import get_spark, BRONZE_PATH, SILVER_PATH

def cleanse_customers(df):
    return (
        df.dropDuplicates(["customer_id"])
          .withColumn("customer_id", col("customer_id").cast(IntegerType()))
          .withColumn("name", trim(col("name")).cast(StringType()))
          .withColumn("email", when(col("email").isNull() | (trim(col("email"))==""),
                                    "unknown").otherwise(lower(trim(col("email")))))
          .withColumn("region", upper(trim(col("region"))))
    )

def cleanse_products(df):
    return (
        df.dropDuplicates(["product_id"])
          .withColumn("product_id", col("product_id").cast(IntegerType()))
          .withColumn("name", trim(col("name")))
          .withColumn("category", upper(trim(col("category"))))
          .withColumn("price", col("price").cast(DecimalType(12,2)))
    )

def cleanse_stores(df):
    return (
        df.dropDuplicates(["store_id"])
          .withColumn("store_id", col("store_id").cast(IntegerType()))
          .withColumn("location", trim(col("location")))
          .withColumn("manager", trim(col("manager")))
    )

def cleanse_orders(df):
    return (
        df.dropDuplicates(["order_id"])
          .withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("customer_id", col("customer_id").cast(IntegerType()))
          .withColumn("store_id", col("store_id").cast(IntegerType()))
          .withColumn("order_date", to_date(col("order_date").cast(StringType())))
          .withColumn("status", upper(trim(col("status"))))
    )

def cleanse_orderlines(df):
    return (
        df.dropDuplicates(["orderline_id"])
          .withColumn("orderline_id", col("orderline_id").cast(IntegerType()))
          .withColumn("order_id", col("order_id").cast(IntegerType()))
          .withColumn("product_id", col("product_id").cast(IntegerType()))
          .withColumn("quantity", col("quantity").cast(IntegerType()))
          .withColumn("unit_price", col("unit_price").cast(DecimalType(12,2)))
          .withColumn("discount", when(col("discount").isNull(), 0.0).otherwise(col("discount")).cast(DecimalType(4,2)))
    )

def main():
    spark = get_spark("Silver_Transform")

    bronze_customers   = spark.read.parquet(f"{BRONZE_PATH}/customers")
    bronze_products    = spark.read.parquet(f"{BRONZE_PATH}/products")
    bronze_stores      = spark.read.parquet(f"{BRONZE_PATH}/stores")
    bronze_orders      = spark.read.parquet(f"{BRONZE_PATH}/orders")
    bronze_orderlines  = spark.read.parquet(f"{BRONZE_PATH}/orderlines")

    silver_customers   = cleanse_customers(bronze_customers)
    silver_products    = cleanse_products(bronze_products)
    silver_stores      = cleanse_stores(bronze_stores)
    silver_orders      = cleanse_orders(bronze_orders)
    silver_orderlines  = cleanse_orderlines(bronze_orderlines)

    silver_customers.write.mode("overwrite").parquet(f"{SILVER_PATH}/customers")
    silver_products.write.mode("overwrite").parquet(f"{SILVER_PATH}/products")
    silver_stores.write.mode("overwrite").parquet(f"{SILVER_PATH}/stores")
    # Partition Orders by year/month for query efficiency
    silver_orders.write.mode("overwrite").partitionBy("status").parquet(f"{SILVER_PATH}/orders")
    silver_orderlines.write.mode("overwrite").parquet(f"{SILVER_PATH}/orderlines")

    print("Silver written.")


if __name__ == "__main__":
    main()