# gold_star.py
from pyspark.sql.functions import col, expr, lit, to_date, sequence, year, month, dayofmonth
from pyspark.sql.types import DateType
from config import get_spark, SILVER_PATH, GOLD_PATH

def build_dim_customer(df):
    return df.select(
        col("customer_id").alias("customer_id"),
        col("name").alias("customer_name"),
        col("email").alias("customer_email"),
        col("region").alias("region")
    )

def build_dim_product(df):
    return df.select(
        col("product_id").alias("product_id"),
        col("name").alias("product_name"),
        col("category").alias("category"),
        col("price").alias("list_price")
    )

def build_dim_store(df):
    return df.select(
        col("store_id").alias("store_id"),
        col("location").alias("store_location"),
        col("manager").alias("store_manager")
    )

def build_dim_date(spark, orders_df):
    # derive min/max from orders
    bounds = orders_df.selectExpr("min(order_date) as min_dt", "max(order_date) as max_dt").collect()[0]
    start = bounds["min_dt"]
    end   = bounds["max_dt"]
    if start is None or end is None:
        # fallback: 1-year calendar if orders empty
        from datetime import date, timedelta
        end = date.today()
        start = date(end.year-1, end.month, end.day)

    calendar = spark.sql(f"select sequence(to_date('{start}'), to_date('{end}'), interval 1 day) as dts") \
                    .selectExpr("explode(dts) as date")

    dim_date = calendar.select(
        col("date").alias("date"),
        year(col("date")).alias("year"),
        month(col("date")).alias("month"),
        dayofmonth(col("date")).alias("day"),
        expr("date_format(date, 'yyyyMMdd')").cast("int").alias("date_id"),
        expr("date_format(date, 'MMMM')").alias("month_name"),
        expr("date_format(date, 'EEE')").alias("weekday_name"),
        expr("quarter(date)").alias("quarter")
    )
    return dim_date

def build_fact_sales(orderlines_df, orders_df):
    # join orderlines to orders
    fact = (
        orderlines_df.alias("ol")
        .join(orders_df.alias("o"), col("ol.order_id")==col("o.order_id"), "inner")
        .select(
            col("ol.orderline_id").alias("orderline_id"),
            col("ol.order_id").alias("order_id"),
            col("o.customer_id").alias("customer_id"),
            col("o.store_id").alias("store_id"),
            col("ol.product_id").alias("product_id"),
            col("o.order_date").alias("order_date"),
            col("o.status").alias("order_status"),
            col("ol.quantity").alias("quantity"),
            col("ol.unit_price").alias("unit_price"),
            col("ol.discount").alias("discount"),
            # Measures
            (col("ol.quantity") * col("ol.unit_price")).alias("gross_amount"),
            (col("ol.quantity") * col("ol.unit_price") * (lit(1.0) - col("discount"))).alias("net_amount")
        )
    )
    return fact

def main():
    spark = get_spark("Gold_Star")

    customers = spark.read.parquet(f"{SILVER_PATH}/customers")
    products  = spark.read.parquet(f"{SILVER_PATH}/products")
    stores    = spark.read.parquet(f"{SILVER_PATH}/stores")
    orders    = spark.read.parquet(f"{SILVER_PATH}/orders")
    orderlines= spark.read.parquet(f"{SILVER_PATH}/orderlines")

    dim_customer = build_dim_customer(customers)
    dim_product  = build_dim_product(products)
    dim_store    = build_dim_store(stores)
    dim_date     = build_dim_date(spark, orders)

    fact_sales   = build_fact_sales(orderlines, orders) \
                    .withColumn("date_id", expr("cast(date_format(order_date,'yyyyMMdd') as int)"))

    # Write Gold Parquet
    dim_customer.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_customer")
    dim_product.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_product")
    dim_store.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_store")
    dim_date.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_date")
    # Partition fact by year for query efficiency
    fact_sales.withColumn("year", expr("year(order_date)")) \
              .write.mode("overwrite").partitionBy("year").parquet(f"{GOLD_PATH}/fact_sales")

    print("Gold written.")


if __name__ == "__main__":
    main()