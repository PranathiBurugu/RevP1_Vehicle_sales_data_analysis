# RevP1_Vehicle_sales_data_analysis

**Downloaded dataset from kaggle**
  Downloaded vehicle sales dataset 
  
**Uploaded csv file to hdfs using hdfs commands in dataproc cluster**
  created directory to store csv file
  moved csv file from local into hdfs directory
  
**Performed cleaning operations**
  created a pyspark file to clean the data i.e., deduplication, handling nulls
  Created SparkSession enabling hive to work with data present in hive tables
  Read the data from csv file by giving hdfs path
  Handled duplicates and nulls
  Stored the data into gcs bucket by giving bucket path
  
**Created external tables in hive**
  Created normalized tables 
  Inserted data into tables using cleaned dataset 
  
**Transferred data to big query**
  Uploaded tables to gcs bucket from hive
  Created external tables in big query using table data present in bucket

**Performed analysis on tables**
  Written queries for analysis
