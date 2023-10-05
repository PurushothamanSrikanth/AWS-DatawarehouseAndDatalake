## This Python file would generate dummy data for the Data Warehouse

import pandas, numpy, os, sys, pyspark
import time, random

pathforDWH = 
pathforDL = 


# Creating a Spark Session
spark = SparkSession\
    .builder\
    .appName("Dummy Data Generation Code")\
    .enableHiveSupport()\
    .getOrCreate()

# Schema for DWH
schema_for_DWH = StructType(
    StructField("id", StringType(), False),
    StructField("Name", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("PAN", StringType(), False),
    StructField("Phone_Number", IntergerType(), False),
    StructField("isStudent", BooleanType(), False),
    StructField("Salary", IntergerType(), False)
)
# Schema for DL
schema_for_DL = StructType(
    StructField("id", StringType(), False),
    StructField("Name", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("PAN", StringType(), False),
    StructField("Phone_Number", StringType(), False),
    StructField("isStudent", StringType(), False),
    StructField("Salary", StringType(), False)
)
while True:
    try:
        # Generate random data for DWH
        data = [
            (
                str(random.randint(1, 100)),
                "Name_" + str(random.randint(1, 10)),
                "Address_"+ str(random.randint(1, 10)),
                "PAN_" + str(random.randint(1, 10)),
                str(random.randint(1000000000, 9999999999)),
                random.choice(["TRUE", "FALSE"]),
                str(random.randint(0, 99999)),
            )
        ]

        # Create a DataFrame from the data and schema
        df = spark.createDataFrame(data, schema = schema_for_DWH)

        # Writing data into the Hive DataWarehouse table
        df.write.mode("append").format("parquet").path(pathforDWH)
        spark.sql('MSCK REPAIR TABLE test.users')

        # Generate random data for DL
        data = [
            (
                str(random.randint(1, 100)),
                "Name_" + str(random.randint(1, 10)),
                "Address_"+ str(random.randint(1, 10)),
                "PAN_" + str(random.randint(1, 10)),
                str(random.randint(1000000000, 9999999999)),
                random.choice(["TRUE", "FALSE"]),
                str(random.randint(0, 99999)),
            )
        ]

        # Create a DataFrame from the data and schema
        df = spark.createDataFrame(data, schema = schema_for_DL)

        # Writing data into the Hive DataWarehouse table
        df.write.mode("append").format("parquet").path(pathforDL)

        time.sleep(1)
    except KeyboardInterrupt:
        spark.stop()
        break