
# UK Microeconomic Data Processing with Apache Spark

This repository contains code to process and analyze the historical microeconomic data of the United Kingdom, dating back to the 13th century. The dataset is processed using Apache Spark, with the data sorted by year in descending order, and key macroeconomic indicators such as Unemployment Rate and Population are extracted and cleaned.

## Overview
The project utilizes Apache Spark on a Google Colab virtual machine to process a large-scale dataset containing UK macroeconomic indicators. The key objective is to extract, clean, and sort the data, particularly focusing on the **Unemployment Rate** and **Population (GB+NI)** over time. The dataset spans several centuries, providing valuable insights into the UK's economic history.

## Setup Instructions

### Prerequisites
To run this code, you will need the following:
- Google Colab (for running the virtual machine)
- Python 3.x
- Apache Spark

### Installing Apache Spark on Google Colab
Follow these steps to set up Apache Spark in your Google Colab environment:

1. **Install Java (required by Spark)**:
    ```bash
    !apt-get install openjdk-8-jdk-headless -qq > /dev/null
    ```

2. **Install Spark**:
    ```bash
    !wget -q https://apache.mirror.digitalpacific.com.au/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
    !tar xf spark-3.1.2-bin-hadoop3.2.tgz
    ```

3. **Set environment variables for Java and Spark**:
    ```bash
    import os
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
    os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop3.2"
    ```

4. **Install PySpark**:
    ```bash
    !pip install -q findspark
    ```

5. **Initialize Spark**:
    ```python
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('UK_Microeconomic_Data').getOrCreate()
    ```

Now your Google Colab environment is ready to use Apache Spark.

## Usage

### Data Loading and Processing
The dataset can be loaded into Spark as follows:

```python
# Load your dataset (CSV, Parquet, etc.)
data = spark.read.csv("path/to/dataset.csv", header=True, inferSchema=True)
```

Once the data is loaded, it can be processed and cleaned. Key columns like `Description`, `Population (GB+NI)`, and `Unemployment rate` are selected, renamed, and sorted by year in descending order.

```python
data = data.withColumnRenamed("Description", 'year') \
           .withColumnRenamed("Population (GB+NI)", "population") \
           .withColumnRenamed("Unemployment rate", "unemployment_rate")

# Sort by year in descending order
data = data.orderBy("year", ascending=False)
data.show()
```

## Cluster Configuration

Apache Spark is configured on a Google Colab virtual machine to access the UK microeconomic dataset. The Spark cluster is set up using the following steps:

1. **Installing and configuring Spark** in the Colab environment.
2. **Setting up environment variables** like `JAVA_HOME` and `SPARK_HOME` to ensure that Spark can be accessed by PySpark.
3. **Initializing Spark** using the `SparkSession` object.

These steps ensure the smooth execution of Spark jobs on a cloud-based virtual machine.

## Data Description

The dataset contains several columns, including:
- **Description (Year)**: The year of the data.
- **Population (GB+NI)**: The population data of Great Britain and Northern Ireland.
- **Unemployment Rate**: The unemployment rate in the UK for each year.

This dataset spans from the 13th century and provides valuable insights into the long-term trends in the UK's macroeconomy.

## Code Explanation

### Key Code Components:
1. **Renaming Columns**: The `withColumnRenamed` method is used to rename columns for better clarity.
2. **Sorting Data**: The `orderBy` method is applied to sort the dataset by the `year` column in descending order.
3. **Handling Null Values**: `dropna` is used to remove rows with missing data.
4. **Broadcasting**: The `broadcast` method is used for optimizing joins with smaller DataFrames.

### Example Code:

```python
from pyspark.sql.functions import col

# Ensure 'year' is of integer type
data = data.withColumn("year", col("year").cast("int"))

# Sort the DataFrame by year in descending order
data_sorted = data.orderBy("year", ascending=False)

# Display the first few rows
data_sorted.show()
```

## Contributing

We welcome contributions to improve this project! Please follow these steps if you'd like to contribute:

1. Fork this repository.
2. Create a new branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature-name`).
5. Create a new Pull Request.
