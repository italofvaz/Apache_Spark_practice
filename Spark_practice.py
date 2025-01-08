# -*- coding: utf-8 -*-
"""Profissao Analista de dados M40 Exercicio.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1-5SSSXFEaTOhMyh1SNo9rF2NsQWJ6r7d

This code sorts the `data` DataFrame by the `year` column in descending order. It first ensures that the `year` column is of integer type and then sorts the DataFrame based on that column, displaying the first few rows of the sorted data using `head()`. The process also involved setting up and configuring an Apache Spark cluster on a Google Colab virtual machine to access the United Kingdom's microeconomic dataset, which dates back to the 13th century.

## 1\. Apache Spark
"""

# Commented out IPython magic to ensure Python compatibility.
# %%capture
# 
# !wget -q https://archive.apache.org/dist/spark/spark-3.0.0/spark-3.0.0-bin-hadoop2.7.tgz
# !tar xf spark-3.0.0-bin-hadoop2.7.tgz && rm spark-3.0.0-bin-hadoop2.7.tgz
#

# Commented out IPython magic to ensure Python compatibility.
# %%capture
# 
# !apt-get remove openjdk*
# !apt-get update --fix-missing
# !apt-get install openjdk-8-jdk-headless -qq > /dev/null

!pip install -q pyspark==3.0.0

"""

It is necessary to configure the machines (nodes) of the cluster so that both the Spark application and the Java installation can be found by PySpark and, consequently, by Python. To achieve this, simply set the environment variables `JAVA_HOME` and `SPARK_HOME` with their respective installation paths."""

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.0.0-bin-hadoop2.7"

"""To connect PySpark (and Python) to Spark and Java, you can use the Python package **FindSpark**."""

!pip install -q findspark==1.4.2

"""The `init()` method injects the environment variables `JAVA_HOME` and `SPARK_HOME` into the Python runtime environment, enabling the proper connection between the PySpark package and the Spark application."""

import findspark

findspark.init()

"""- **master**: Address (local or remote) of the cluster.  
- **appName**: Name of the application.  
- **getOrCreate**: Method that actually creates the resources and instantiates the application.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("pyspark-notebook").getOrCreate()

"""With the `SparkSession` object properly instantiated, we can start interacting with the data using the cluster's resources through a data structure we are already familiar with: **DataFrames**.

## 2\. Data Wrangling

**2.1\. Data**
"""

!wget -q "https://raw.githubusercontent.com/cluster-apps-on-docker/spark-standalone-cluster-on-docker/master/build/workspace/data/uk-macroeconomic-data.csv" -O "uk-macroeconomic-data.csv"

data = spark.read.csv(path="uk-macroeconomic-data.csv", sep=",", header=True )

data.show()

data.printSchema()

"""**2.2. Wrangling**

In this study, our objective is to process the data so that the final database presents the values for the **Unemployment rate** and **Population (GB+NI)** ordered by year in descending order.

- Using Pandas
"""

import pandas as pd

df = pd.read_csv("uk-macroeconomic-data.csv", sep=",")

df.head()

df = df[["Description", "Population (GB+NI)", "Unemployment rate"]]

df.head()

df = df.rename(columns={
    "Description": "year",
    "Population (GB+NI)": "population",
    "Unemployment rate": "unemployment_rate"
})

df_sort = df.sort_values(by="year", ascending=False)

df_sort.head()

""" - Using PySpark:"""

data.count()

data.columns

"""The `select` method selects columns from the DataFrame, while the `withColumnRenamed` method renames columns."""

data = data.select([ "Description", "Population (GB+NI)", "Unemployment rate" ])

data = data.\
withColumnRenamed ("Description", 'year' ).\
withColumnRenamed( "Population (GB+NI)", "population" ).\
withColumnRenamed( "Unemployment rate", "unemployment_rate" )

data.show(n=10)

data = data.orderBy("year", ascending=False)
data.show()