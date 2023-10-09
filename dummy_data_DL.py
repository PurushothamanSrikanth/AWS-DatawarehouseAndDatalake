## This Python file would generate dummy data for the Data Warehouse

import pandas, numpy, os, sys, pyspark
import time, random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import BooleanType, IntegerType, StringType, DecimalType, LongType
from pyspark import SparkConf
import findspark
findspark.init()

pathforDL = 's3://dwanddl/user/'

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.3')
conf.set('spark.jars.packages', 'com.amazonaws:aws-java-sdk-bundle:1.11.901')
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-common:3.3.3')

# Creating a Spark Session
spark = SparkSession\
    .builder\
    .appName("Dummy Data Generation Code")\
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .enableHiveSupport()\
    .getOrCreate()

# Schema for DL
schema_for_DL = StructType([
    StructField("id", StringType(), False),
    StructField("Name", StringType(), False),
    StructField("Address", StringType(), False),
    StructField("PAN", StringType(), False),
    StructField("Phone_Number", StringType(), False),
    StructField("isStudent", StringType(), False),
    StructField("Salary", StringType(), False),
    StructField("Month", StringType(), False)
])

for i in range(20):
    try:
        # Generate random data for DL
        data = [
            (
                str(random.randint(1, 100)),
                str("Name_" + str(random.randint(1, 10))),
                str("Address_" + str(random.randint(1, 10))),
                str("PAN_" + str(random.randint(1, 10))),
                str(random.randint(1000000000, 9999999999)),
                str(random.choice([True, False])),
                str(random.randint(0, 99999)),
                str(random.choice(["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"])
            ))
        ]

        # Creating a DataFrame from the data and schema
        df = spark.createDataFrame(data, schema=schema_for_DL)

        # Writing data into the Hive DataWarehouse table
        df.repartition("Month").write.mode("overwrite").parquet(pathforDL)

        time.sleep(1)
    except KeyboardInterrupt:
        spark.stop()
        break