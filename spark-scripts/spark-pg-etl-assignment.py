import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, when
from dotenv import load_dotenv

# Load environment variables
dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

# Initialize Spark
sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
    pyspark
    .SparkConf()
    .setAppName('Dibimbing')
    .setMaster(spark_host)
    .set("spark.jars", "/opt/bitnami/spark/jars/postgresql-42.2.18.jar")
))
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:5432/{postgres_dw_db}'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver',
    'stringtype': 'unspecified'
}

# Path to the folder containing the CSV files
data_folder = Path("/data/olist")

# Get a list of all CSV files in the folder
csv_files = list(data_folder.glob("*.csv"))

# **Initialize the list to store table names for later verification**
table_names = []  # <--- Make sure this line is present

# Loop through each CSV file and ingest into PostgreSQL
for csv_file in csv_files:
    # Derive the table name directly from the file name (no need to replace hyphens)
    table_name = f"public.{csv_file.stem}"  # Use the file name (without extension) as the table name
    table_names.append(table_name)  # Add the table name to the list for later use

    print(f"Processing file: {csv_file.name} -> Table: {table_name}")

    # Infer schema automatically
    df = spark.read.csv(str(csv_file), header=True, inferSchema=True)

    # Write the DataFrame to PostgreSQL
    (
        df
        .write
        .mode("overwrite")  # Use overwrite mode to replace the table
        .jdbc(
            jdbc_url,
            table_name,
            properties=jdbc_properties
        )
    )
    print(f"Data from {csv_file.name} has been ingested into {table_name}")

print("\n==== Data Ingestion Complete ====\n")

# Optionally, read back and display the data from all tables to verify
for table in table_names:
    print(f"Preview of data from table {table}:")
    try:
        result_df = spark.read.jdbc(
            jdbc_url,
            table,
            properties=jdbc_properties
        )
        result_df.show(5)  # Display first 5 rows
    except Exception as e:
        print(f"Failed to read table {table}. Error: {e}")
    print("\n-----------------------------------\n")