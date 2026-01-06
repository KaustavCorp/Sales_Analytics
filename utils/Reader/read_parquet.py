from pyspark.sql import *
from Jobs.config import get_spark


def main():
    spark = get_spark("Read_Parquet")
    df = spark.read.format("parquet").load("D:\Demo\Sales_Analytics\Data\Bronze\customers")
    df.show(10,False)

if __name__ == "__main__":
    main()