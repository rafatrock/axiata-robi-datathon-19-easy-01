# -*- coding: utf-8 -*-
"""robi-datathon-19-easy-01

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1YwfVTKhOqewZK5E23RBBWdzqvdp0DjP_

#Step 01. Install All Dependencies

This installs Apache Spark 2.3.3, Java 8, Findspark and most importnantly CSVKit library that makes it easy for Python to work on the given Big Data.
"""

#OpenJDK Dependencies for Spark
!apt-get install openjdk-8-jdk-headless -qq > /dev/null 

#Download Apache Spark
!wget -q http://apache.osuosl.org/spark/spark-2.3.3/spark-2.3.3-bin-hadoop2.7.tgz 

#Apache Spark and Hadoop Unzip
!tar xf spark-2.3.3-bin-hadoop2.7.tgz 

#FindSpark Install
!pip install -q findspark 

#CSVKit for Handling and Merging CSV Files
!pip install -q csvkit

"""# Step 02. Set Environment Variables
Set the locations where Spark and Java are installed based on your installation configuration. Double check before you proceed.
"""

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.3-bin-hadoop2.7"

"""# Step 03. Start a Spark Session
This basic code will prepare to start a Spark session.
"""

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ml-datathon19-easy01').master("local[4]").getOrCreate()