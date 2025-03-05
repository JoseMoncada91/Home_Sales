# Home_Sales

PySpark Home Sales Analysis
Overview
This project uses PySpark to perform data analysis on home sales data from a CSV file hosted on AWS S3. The analysis includes various SQL queries to explore and manipulate home sales data, such as calculating the average price of homes based on specific criteria (e.g., number of bedrooms, year built, view rating). The script demonstrates the following tasks:

Loading data from an S3 bucket into a PySpark DataFrame.
Data transformation and type casting (e.g., ensuring the date_built field is in DateType).
Running SQL queries for aggregating and analyzing home sales data.
Caching temporary tables for performance optimization.
Using Parquet files for efficient storage and querying.
Prerequisites
Python 3.6 or higher
PySpark 3.x or higher
AWS S3 access (for fetching the CSV file)
Installation
Install PySpark using pip:
bash
Copy
Edit
pip install pyspark
Download and install Hadoop (if not already installed) to run Spark in local mode. You can follow the instructions in the official PySpark documentation.
Script Breakdown
Step 1: Initialize Spark
The script begins by initializing the Spark environment using the findspark library. This allows PySpark to connect to the local Spark instance, enabling distributed data processing.

Step 2: Create SparkSession
A SparkSession is created. This is the entry point for using Spark with DataFrames and SQL in PySpark. It enables interaction with structured data and the execution of SQL queries.

Step 3: Read CSV Data from S3
The script reads a CSV file (home_sales_revised.csv) from an AWS S3 bucket into a PySpark DataFrame. The file is first added to the Spark file system using SparkContext.addFile and then loaded into a DataFrame for further analysis.

Step 4: Data Transformation
Data transformations are performed on the date_built column to ensure it is in the correct date format. The column is concatenated to form a valid date and then cast to the DateType. This transformation ensures that the data is consistent and ready for further analysis.

Step 5: SQL Queries
The script runs multiple SQL queries on the DataFrame to analyze home sales data. These queries calculate the average price of homes based on different conditions like the number of bedrooms, the year a home was built, and specific features like the number of floors or square footage. SQL is used for data aggregation and summarization.

Step 6: Caching and Performance Optimization
To improve the performance of repeated queries, the script caches the home_sales DataFrame. Caching stores the DataFrame in memory, enabling faster query execution since the data is not read from disk multiple times.

Step 7: Parquet Data Partitioning
The script attempts to write the DataFrame to a Parquet file while partitioning the data by the date_built field. Parquet is a columnar storage format that is efficient for querying large datasets. Partitioning helps to organize the data based on the date_built column, which improves query performance on large datasets.

Step 8: Performance Comparison
The script compares the runtime of SQL queries executed on cached data and uncached data. It measures the time it takes to run the queries both with and without caching, highlighting the performance improvements achieved by caching data in memory.

Issue with Item #10: Partitioning by date_built and Writing to Parquet
While attempting to partition the data by the date_built field and write it to a Parquet file, an error occurred.
The following error message was encountered:

Py4JJavaError: An error occurred while calling o246.write.
: org.apache.spark.sql.AnalysisException: Partition column must be a non-nullable column

Steps Taken to Resolve the Issue
Checked column data types: We verified that the date_built column was correctly cast to a DateType, which is a nullable column. This led us to believe that partitioning on a nullable column might be the cause of the issue.

Tried casting date_built to StringType: We attempted to cast the date_built column to a non-nullable StringType column, hoping it would resolve the partitioning issue. However, this did not work, and the error persisted.

Partitioning with other columns: We experimented with partitioning by other non-nullable columns, such as price and bedrooms. These partitions worked fine, confirming the issue was specific to date_built.

Reviewed PySpark and Spark versions: We checked the compatibility of PySpark and the underlying Spark version. The issue appeared to be related to partitioning on nullable columns in some versions of Spark.

Laptop Setup Steps:

In order to run PySpark on Windows, several environment variables and permissions were adjusted to allow proper execution:

Set environment variables for Spark and Hadoop:

Added the SPARK_HOME variable to point to the location of Spark on the machine.
Set the HADOOP_HOME variable to the folder containing Hadoop binaries to ensure proper interaction between Spark and Hadoop.
Added Spark to the system PATH:

Appended the bin folder within SPARK_HOME to the system PATH variable, so that commands such as spark-submit are recognized in the command prompt.
Allowed full access to winutils.exe:

The script failed initially due to permission issues with the winutils.exe file, which is necessary for Hadoop to run on Windows.
We navigated to the winutils.exe file in the Hadoop bin folder and changed its file permissions to "Allow" full control for the user to ensure Spark could run without permission errors.
Ensured the Java environment is correctly set up:

Set the JAVA_HOME environment variable to the path of the JDK installation, ensuring that Spark could utilize the correct version of Java for its operations.
Conclusion:

Unfortunately, we were unable to resolve the issue with partitioning by the date_built field. The error seems to be related to partitioning on nullable columns, and more research is needed to identify a definitive solution. In the meantime, we were able to proceed with other tasks in the script that did not require partitioning by date_built.

