# Importing necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import HiveContext

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BigDataProcessingPipeline") \
    .config("spark.sql.warehouse.dir", "hdfs://your-hadoop-cluster-path") \
    .enableHiveSupport() \
    .getOrCreate()

# Create Hive Context to interact with Hive
hive_context = HiveContext(spark.sparkContext)

# Load data from HDFS into Spark DataFrame
df = spark.read.csv("hdfs://your-hadoop-cluster-path/input-data/*.csv", header=True, inferSchema=True)

# Data Preprocessing
# Example: Filter out rows where the 'value' column is null and cast 'timestamp' column to timestamp type
df_clean = df.filter(col("value").isNotNull())  # Removing rows where 'value' column is null
df_clean = df_clean.withColumn("timestamp", col("timestamp").cast("timestamp"))  # Casting 'timestamp' to timestamp type

# Data Transformation using Spark SQL
df_clean.createOrReplaceTempView("temp_table")
spark.sql("SELECT key, SUM(value) AS total_value FROM temp_table GROUP BY key").show()

# Perform data processing and transformation using Hive
# Creating Hive external table
hive_context.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS big_data_table (
        key STRING,
        value DOUBLE,
        timestamp TIMESTAMP
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs://your-hadoop-cluster-path/output-data/'
""")

# Inserting data into Hive table
df_clean.write.insertInto("big_data_table")

# Optimizing Spark jobs using partitioning
# Repartition data based on the 'key' column to improve performance
df_clean = df_clean.repartition(10, "key")  # Repartition into 10 partitions based on 'key' column

# Persist the cleaned data to memory to avoid re-computation
df_clean.persist()

# Perform an aggregation query using Hive
query = """
    SELECT key, AVG(value) AS average_value 
    FROM big_data_table 
    WHERE timestamp BETWEEN '2024-01-01' AND '2024-12-31' 
    GROUP BY key
"""
result = spark.sql(query)
result.show()

# Optimize the final query performance using caching
result.cache()

# Saving the final processed results back to HDFS in Parquet format for optimized storage
result.write.mode("overwrite").parquet("hdfs://your-hadoop-cluster-path/processed-results/")

# Alternative: Save back to HDFS in CSV format
result.write.mode("overwrite").csv("hdfs://your-hadoop-cluster-path/processed-results-csv/")

# Using Spark RDDs for more complex data transformation
# Example: Doubling the 'value' column using RDD operations
rdd = df_clean.rdd.map(lambda row: (row['key'], row['value'] * 2))  # Doubling the 'value' column
processed_rdd = rdd.collect()  # Collecting the transformed data

# Show the results of RDD transformation
for item in processed_rdd:
    print(item)

# Finalizing the Spark session
spark.stop()  
