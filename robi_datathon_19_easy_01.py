#!/usr/bin/env python
# Step 02. Set Environment Variables

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.3-bin-hadoop2.7"

# Step 03. ELT - Load the Data: Mega Cloud Access

import os
os.system('git clone https://github.com/jeroenmeulenaar/python3-mega.git')
os.chdir('python3-mega')
os.system('pip install -r requirements.txt')

# Step 04. ELT - Load the Data: Read Uploaded Dataset

from mega import Mega
os.chdir('../')
m_revenue = Mega.from_ephemeral()
m_revenue.download_from_url('https://mega.nz/#!1lJH3Q4K!N94-KRSyn22FPb0yxiVJgndjxUStdlfC2_prWDYI2f0')

# Step 05. Start a Spark Session

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ml-datathon19-easy01').master("local[4]").getOrCreate()

# Step 06. Exploration - Data Schema View

df = spark.read.csv('revenue.csv', header = True, inferSchema = True)
df.printSchema()

# Step 07. Exploration - Data Type Overview

df.dtypes

# Step 08. Exploration - More Statistical Insights from Data

df.describe().show()

# Step 09. Exploration - Total Unique Row Count

print("Unique Rows: ")
df.distinct().count()

# Step 10. Implementation - Run the SQL Command

from pyspark.sql.functions import desc

df2 = df.drop(df.week_number)

Easy01 = df2.groupBy('msisdn').agg({'revenue_usd':'sum'}).sort(desc("sum(revenue_usd)"))

Easy01 = Easy01.withColumnRenamed("sum(revenue_usd)", "TotalRevenue_USD")
Easy01 = Easy01.withColumnRenamed("msisdn", "User")

Easy01.show(n=5, truncate=False)

# End of code
