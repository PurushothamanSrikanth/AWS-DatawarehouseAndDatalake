## This Python file would generate dummy data for the Data Warehouse

import pandas, numpy, os, sys, pyspark
import time, random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import BooleanType, IntegerType, StringType, DecimalType, LongType
import findspark
findspark.init()

pathforDWH = '/user/hive/warehouse/test'

# Creating a Spark Session
spark = SparkSession\
    .builder\
    .appName("Dummy Data Generation Code")\
    .enableHiveSupport()\
    .getOrCreate()

# Schema for DWH
schema_for_DWH = StructType([
    StructField("id", StringType(), False),
    StructField("Name", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("PAN", StringType(), False),
    StructField("Phone_Number", LongType(), False),
    StructField("isStudent", BooleanType(), False),
    StructField("Salary", IntegerType(), False),
    StructField("Month", StringType(), False)
])

while True:
    try:
        # Generate random data for DWH
        data = [
            (
                str(random.randint(1, 100)),
                str("Name_" + str(random.randint(1, 10))),
                str("Address_"+ str(random.randint(1, 10))),
                str("PAN_" + str(random.randint(1, 10))),
                random.randint(1000000000, 9999999999),
                random.choice([True, False]),
                random.randint(0, 99999),
                str(random.choice(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
            ))
        ]

        # Creating a DataFrame from the data and schema
        df = spark.createDataFrame(data, schema = schema_for_DWH)

        # Writing data into the Hive DataWarehouse table
        df.repartition("Month").write.mode("overwrite").format("parquet").path(pathforDWH)
        spark.sql('MSCK REPAIR TABLE test.users')

        time.sleep(1)
    except KeyboardInterrupt:
        spark.stop()
        break