# Rev_Vehicle_sales_data_analysis

**Downloaded dataset from kaggle**
  Downloaded vehicle sales dataset 


**P1**

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

  Uploaded tables to bigquery using pyspark job

**Performed analysis on tables**

  Written queries for analysis
  Used matplotlib and seaborn for visualization of queries in jupyter notebook



**P2**

**Uploaded CSV File to GCS Bucket**

  Created a Cloud Storage bucket to act as the data source.
  Uploaded sample CSV files periodically to simulate real-time ingestion.
  Followed a naming pattern: data_<timestamp>.csv.

**Orchestrated Pipeline with Cloud Composer**

  Created a Cloud Composer environment for workflow orchestration.
  Developed an Apache Airflow DAG:
    Scheduled to run every 10 minutes.
    Used a custom logic to detect new CSV files in the bucket.
    Published file metadata to a Pub/Sub topic when new files were detected.

**Set Up Pub/Sub Messaging Layer**

  Created a Pub/Sub topic to handle event notifications.
  Airflow DAG published messages containing CSV metadata to the topic.
  Created a Pub/Sub subscription for the downstream Dataflow pipeline.

**Transformed and Ingested Data Using Dataflow**

  Created a Dataflow pipeline using Apache Beam (Python).
  Consumed messages from the Pub/Sub subscription.
  Read and parsed the CSV file from GCS using metadata.
  Performed data cleaning and transformation (e.g., type casting, formatting).
  Loaded the cleaned data into a BigQuery table in append mode.

**Loaded Data into BigQuery**

  Created a database in BigQuery then the table is automatically exported from pipeline.


