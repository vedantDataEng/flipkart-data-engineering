# Databricks notebook source
# Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, sum, avg

# COMMAND ----------

# Start Spark Session
spark = SparkSession.builder.appName("Flipkart Data Engineering").getOrCreate()


# COMMAND ----------

# File Path
file_path = "/Volumes/flipkartproduct/default/data/file/Flipkart.csv"

# COMMAND ----------

# Step 1: Data Ingestion
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)
flipkart_df.printSchema()
flipkart_df.describe().show()
flipkart_df.display()

# COMMAND ----------

# Step 2: Data Quality Check (Missing values count)
flipkart_df.select([count(when(col(c).isNull(), c)).alias(c) for c in flipkart_df.columns]).display()

# COMMAND ----------

# Step 3: Handle Missing Values
flipkart_df_clean = flipkart_df.dropna()
flipkart_df_filled = flipkart_df.fillna({"Rating": 0, "maincateg": "Men"})


# COMMAND ----------

# Step 4: Deduplication (Remove duplicates based on title + price)
flipkart_df_dedup = flipkart_df_filled.dropDuplicates(["title", "actprice1"])
flipkart_df_dedup.display()



# COMMAND ----------

# Step 5: Transformation (Add Effective Price Column)
flipkart_df_transformed = flipkart_df_dedup.withColumn("EffectivePrice", col("actprice1"))
display(flipkart_df_transformed)

# COMMAND ----------

# Step 6: Business Logic
high_rated_products = flipkart_df_transformed.filter(col("Rating") > 4)
display(high_rated_products)

# COMMAND ----------

avg_rating_by_category = flipkart_df_transformed.groupBy("maincateg").agg(avg("Rating").alias("avg_rating"))
avg_rating_by_category.display()


# COMMAND ----------

total_revenue_by_category = flipkart_df_transformed.groupBy("maincateg").agg(sum("actprice1").alias("total_revenue"))
total_revenue_by_category.display()

# COMMAND ----------

# Step 7: Incremental Load Simulation (Append Mode instead of overwrite)


output_table = "Flipkart_Data_Analysis_table"
flipkart_df_transformed.write.mode("append").saveAsTable(output_table)


# COMMAND ----------

# Step 8: Partitioning for Performance (Partition by category)
flipkart_df_transformed.write.mode("overwrite").partitionBy("maincateg").saveAsTable("Flipkart_Data_Analysis_partitioned")
flipkart_df_transformed.display()

# COMMAND ----------

# Step 9: External Storage (simulate writing to S3/ADLS instead of only Delta)

flipkart_df_filled.write.format("delta").mode("append").saveAsTable(output_table)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Flipkart_Data_Analysis_table limit 20 

# COMMAND ----------

