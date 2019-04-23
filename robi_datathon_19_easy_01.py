#!/usr/bin/env python

__author__ = "Rubaiyat Islam Rafat"
__copyright__ = "Copyright 2019, Eagle Eye Lab"
__credits__ = ["Rubaiyat Islam Rafat"]
__license__ = "MIT"
__version__ = "1.0.1"
__maintainer__ = "Rubaiyat Islam Rafat"
__email__ = "rob@spot.colorado.edu"
__status__ = "Production"

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-2.3.3-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('ml-datathon19-easy01').master("local[4]").getOrCreate()
