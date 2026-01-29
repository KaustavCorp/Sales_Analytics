from opt.airflow.jobs.config import get_spark



def main():
    spark = get_spark("Read_Parquet")
    #You can change the file names in load command below to read different files.
    df = spark.read.format("parquet").load(r"D:\Demo\Sales_Analytics\opt\airflow\data\Gold\dim_customer")
    df.show()

if __name__ == "__main__":
    main()