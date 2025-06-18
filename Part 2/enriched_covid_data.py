#!/usr/bin/env python3
# enriched_covid_data.py
# -------------------------------------------------------------
# Spark job: Reads cleaned data from MongoDB, enriches it with
# daily new cases/deaths, and saves the result to HDFS.
# -------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lag, when
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------

# Input from MongoDB
MONGO_INPUT_URI = "mongodb://localhost:27017/covid_project.cleaned_data"

# Output location on HDFS
OUT_HDFS_PATH = "/user/alexss/group_project/enriched_covid_data"


# ------------------------------------------------------------------
# 1. SPARK SESSION
#    Using the optimized configurations we found earlier.
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("COVID-19 Data Enrichment")
    .config("spark.sql.shuffle.partitions", 8)  # Use 8 shuffle partitions
    .getOrCreate()
)

# ------------------------------------------------------------------
# 2. LOAD DATA FROM MONGODB
#    Using the single partitioner for efficient reading.
# ------------------------------------------------------------------
print(f"Reading data from MongoDB URI: {MONGO_INPUT_URI}")

df = spark.read.format("mongo") \
    .option("uri", MONGO_INPUT_URI) \
    .option("spark.mongodb.input.partitioner", "MongoSinglePartitioner") \
    .load()

# ------------------------------------------------------------------
# 3. INITIAL DATA PREPARATION
# ------------------------------------------------------------------
print("\nSuccessfully loaded data from MongoDB.")

# Cast date string → date type for proper sorting and functions
df_prepared = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

# Cache the DataFrame as it will be used for multiple calculations
df_prepared.cache()

print("Schema of the loaded data:")
df_prepared.printSchema()

print("\nSample of the loaded data:")
df_prepared.show(10)


# --------------------------------------------------------------------------------
# --- YOUR ANALYSIS & AUGMENTATION LOGIC STARTS HERE ---
#    (We will review this section by section)
# --------------------------------------------------------------------------------

# Define window for calculations partitioned by country/state and ordered by date
window_spec = Window.partitionBy("country", "state").orderBy("date")

# Add columns for previous day's confirmed cases and deaths
df_with_prev = (
    df_prepared
    .withColumn("prev_confirmed", lag("confirmed").over(window_spec))
    .withColumn("prev_deaths", lag("deaths").over(window_spec))
)

# Calculate daily new values based on the difference from the previous day
df_with_daily = (
    df_with_prev
    .withColumn("daily_new_cases", (col("confirmed") - col("prev_confirmed")))
    .withColumn("daily_new_deaths", (col("deaths") - col("prev_deaths")))
)

# Replace any nulls that appeared (e.g., for the first day of data) with 0
df_enriched = (
    df_with_daily
    .withColumn("daily_new_cases", when(col("daily_new_cases").isNull(), 0).otherwise(col("daily_new_cases")))
    .withColumn("daily_new_deaths", when(col("daily_new_deaths").isNull(), 0).otherwise(col("daily_new_deaths")))
)

# Show a sample of the final enriched data
print("\nEnriched data with daily new cases/deaths:")
df_enriched.select("date", "country", "state", "confirmed", "daily_new_cases", "deaths", "daily_new_deaths").show(10)


# ------------------------------------------------------------------
# 4. SAVE ENRICHED DATA TO HDFS
# ------------------------------------------------------------------
print(f"\nSaving enriched data to HDFS at: {OUT_HDFS_PATH}")

# Saving as Parquet is a best practice for performance
df_enriched.write.mode("overwrite").parquet(OUT_HDFS_PATH)

print("\n✅ Enrichment job complete.")
spark.stop()