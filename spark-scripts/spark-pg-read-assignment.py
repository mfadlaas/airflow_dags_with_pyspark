import os
from pathlib import Path
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp,col,when
from dotenv import load_dotenv

dotenv_path = Path('/opt/app/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_dw_db = os.getenv('POSTGRES_DW_DB')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

spark_host = "spark://dibimbing-dataeng-spark-master:7077"

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

# Top 5 Cities with highest revenue
query = """
            SELECT 
                result.customer_city,
                result.customer_state,
                result.Total_Orders AS Total_Orders,
                result.Total_Customers_Payment AS Total_Customers_Payment
            FROM (
                SELECT
                    raw_data.customer_city,
                    raw_data.customer_state,
                    COUNT(DISTINCT raw_data.order_id) AS Total_Orders,
                    SUM(raw_data.payment_value) AS Total_Customers_Payment
                FROM (
                    SELECT
                        c.customer_unique_id,
                        c.customer_id,
                        o.order_id,
                        c.customer_city,
                        c.customer_state,
                        o.order_status,
                        o.order_delivered_customer_date,
                        p.payment_value
                    FROM 
                        olist_customers_dataset c
                    JOIN 
                        olist_orders_dataset o 
                        ON c.customer_id = o.customer_id
                    JOIN 
                        olist_order_payments_dataset p 
                        ON o.order_id = p.order_id
                    WHERE 
                        o.order_status = 'delivered'
                ) AS raw_data
                GROUP BY 
                    raw_data.customer_city, 
                    raw_data.customer_state
                ORDER BY 
                    Total_Customers_Payment DESC
            ) AS result
            ORDER BY 
                result.Total_Customers_Payment DESC
            LIMIT 5
        """


# Wrap the query in parentheses and provide an alias
wrapped_query = f"({query}) AS customer_alias"

# Read the joined data
joined_df = spark.read.jdbc(
    url=jdbc_url,
    table=wrapped_query,
    properties=jdbc_properties
)

print("Top 5 Cities with Highest Revenue")
# Display the data
joined_df.show(5)
