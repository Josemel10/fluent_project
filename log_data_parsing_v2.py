# Databricks notebook source
# MAGIC %md
# MAGIC ###  I have demonstrated my approach to parse a log based data to a structured table format for our business needs, and handling of the dynamic schema evolution logic 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log data preview

# COMMAND ----------

2024-11-20 15:45:00 INFO event_id=e123 customer_id=c001 event_type=purchase product_id=p123 product_name="Smartphone" product_category=Electronics product_price=699.99 purchase_date=2024-11-20 customer_location="New York" session_duration=300 review_score=null
2024-11-20 16:00:00 INFO event_id=e124 customer_id=c002 event_type=purchase product_id=p124 product_name="Laptop" product_category=Electronics product_price=1299.99 purchase_date=2024-11-20 customer_location="San Francisco" session_duration=600 review_score=4.5
2024-11-20 16:30:00 INFO event_id=e125 customer_id=c001 event_type=review product_id=p123 product_name="Smartphone" product_category=Electronics review_score=5.0 event_timestamp=2024-11-20
2024-11-21 10:00:00 INFO event_id=e126 customer_id=c003 event_type=return product_id=p125 product_name="Headphones" product_category=Accessories return_status="initiated" event_timestamp=2024-11-21

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading the log based data

# COMMAND ----------


raw_logs = spark.read.text("dbfs:/FileStore/log_data.txt")
raw_logs.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Parsing the log data using Regular_Expression

# COMMAND ----------

from pyspark.sql.functions import regexp_extract

# Defining regex patterns - source sample data would help to find the pattern of the incoming data
patterns = {
    "event_id": r"event_id=([^\s]+)",
    "customer_id": r"customer_id=([^\s]+)",
    "event_type": r"event_type=([^\s]+)",
    "product_id": r"product_id=([^\s]+)",
    "product_name": r'product_name="([^"]+)"',
    "product_category": r"product_category=([^\s]+)",
    "product_price": r"product_price=([^\s]+)",
    "purchase_date": r"purchase_date=([^\s]+)",
    "customer_location": r'customer_location="([^"]+)"',
    "session_duration": r"session_duration=([^\s]+)",
    "review_score": r"review_score=([^\s]+)",
    "return_status": r'return_status="([^"]+)"',
    "event_timestamp": r"event_timestamp=([^\s]+)"
}

# Applying regex patterns to extract fields (looping through the defined pattern and the df, to create a new df with extrated column and its value)
parsed_logs = raw_logs
for column, pattern in patterns.items():
    parsed_logs = parsed_logs.withColumn(column, regexp_extract("value", pattern, 1))

parsed_logs.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Null and Missing Values

# COMMAND ----------

# sample code of imputing the missing values with default, and casting of columns

from pyspark.sql.functions import when, col

parsed_logs = parsed_logs.withColumn("product_price", when(col("product_price") == "null", None).otherwise(col("product_price").cast("double"))) \
                         .withColumn("review_score", when(col("review_score") == "null", 0).otherwise(col("review_score").cast("double"))) \
                         .withColumn("session_duration", col("session_duration").cast("int"))


parsed_logs = parsed_logs.filter(col("event_id").isNotNull() & col("customer_id").isNotNull())
parsed_logs = parsed_logs.drop("value") # dropping the actual raw_log column from which we extracted the values

parsed_logs.show(truncate=False)


# COMMAND ----------

parsed_logs.createOrReplaceTempView("view1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from view1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing it to the dbfs

# COMMAND ----------

parsed_logs.write.format("delta").mode("overwrite").save("dbfs:/FileStore/parsed_logs")

spark.sql("CREATE TABLE IF NOT EXISTS parsed_logs USING DELTA LOCATION 'dbfs:/FileStore/parsed_logs'")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling schema evolution 

# COMMAND ----------

# MAGIC %md
# MAGIC Schema evolution in JSON format can be handled easily by using inbuilt spark function like infer and merge schema, and by setting schema evolution mode in case of structured streaming
# MAGIC
# MAGIC BUT,,,,
# MAGIC
# MAGIC LOG data in a text format cannot be inferred by the spark, so we are configuring it in a key-value pair in dictionary as predefined patterns of the log data columns

# COMMAND ----------

patterns= {
  "event_id": "event_id=([^\\s]+)",
  "customer_id": "customer_id=([^\\s]+)",
  "event_type": "event_type=([^\\s]+)",
  "product_id": "product_id=([^\\s]+)",
  "product_name": "product_name=\"([^\"]+)\"",
  "product_category": "product_category=([^\\s]+)",
  "product_price": "product_price=([^\\s]+)",
  "purchase_date": "purchase_date=([^\\s]+)",
  "customer_location": "customer_location=\"([^\"]+)\"",
  "session_duration": "session_duration=([^\\s]+)",
  "review_score": "review_score=([^\\s]+)",
  "return_status": "return_status=\"([^\"]+)\"",
  "event_timestamp": "event_timestamp=([^\\s]+)"
}


# COMMAND ----------

from pyspark.sql.functions import regexp_extract, col, lit

# Extracting all predefined columns using patterns stored in dictionary(same logic as above use case)
parsed_logs = raw_logs
for column, pattern in patterns.items():
    parsed_logs = parsed_logs.withColumn(column, regexp_extract(col("value"), pattern, 1))

# Extracting all available fields from incoming log data
log_fields = (
    raw_logs.select("value")
    .rdd.flatMap(lambda row: [field.split("=")[0] for field in row[0].split()]) #This logic splits each row by spaces, then splits each key-value pair by "=" to extract and return just the keys.
    .distinct()
    .collect()
)

# Finding extra columns which are not configured in predefined patterns
extra_columns = list(set(log_fields) - set(patterns.keys()))

# Dynamically extracting teh values for extra columns 
for column in extra_columns:
    # here the pattern for the extra columns are created, where {} replaces the column, and value after '=' sign is taken as the value of the column
    extra_column_pattern = r"{}=([^\s]+)".format(column)    
    
    # Adding all the newly extracted column into the df
    parsed_logs = parsed_logs.withColumn(column, regexp_extract(col("value"), extra_column_pattern, 1))

# Show the final DataFrame
parsed_logs.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate and Update extra columns into predefined JSON 

# COMMAND ----------

# here I am writing the extra fileds into a json, to further validate and add it to the existing predefined patterns for future data 
extra_fields_df = parsed_logs.select(*extra_columns)
extra_fields_df.write.format("json").save("dbfs:/FileStore/new_fields.json")


# COMMAND ----------

# MAGIC %md
# MAGIC To conclude the schema evolution in log data, we can have a default number of columns that has to be extracted from the source, then we can have the extra fields added in real time to the predefined pattern.
