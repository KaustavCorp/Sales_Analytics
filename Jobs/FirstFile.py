from pyspark.sql import *
from pyspark.sql.types import StructField, IntegerType, StringType, StructType

spark = SparkSession.builder\
                    .appName("FirstTest")\
                    .master("local[2]")\
                    .getOrCreate()

input_data = [("Kaustav",33,"Burnpur"),
              ("Papri",30,"Asansol")]
input_schema = StructType([StructField("Name",StringType(),False),
                    StructField("Age",IntegerType(),False),
                    StructField("Location",StringType(),False)])
df = spark.createDataFrame(input_data,input_schema)
df.show()