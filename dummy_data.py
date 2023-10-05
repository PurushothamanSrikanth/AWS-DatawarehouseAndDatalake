## This Python file would generate dummy data for the Data Warehouse

import pandas, numpy, os, pyspark
import time, random

# Creating a Spark Session
spark = SparkSession\
    .builder\
    .appName("Dummy Data Generation Code")\
    .enableHiveSupport()\
    .getOrCreate()

# Schema
schema = StructType(
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
        # Generate random data
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
        df = spark.createDataFrame(data, schema= schema)

        # Writing data into the Hive table
        df.write.mode("append").saveAsTable("user")

        time.sleep(120) # Sleep for 2 minutes
    except KeyboardInterrupt:
        spark.stop()
        break
