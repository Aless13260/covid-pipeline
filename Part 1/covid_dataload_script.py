# ==============================================================================
# 1. SETUP: Import libraries and create the SparkSession
# ==============================================================================
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, expr

# This is the entry point for any Spark application.
spark = SparkSession.builder \
    .appName("CovidDataCombination") \
    .getOrCreate()

print("SparkSession created. Starting data processing...")

# ==============================================================================
# 1. PROCESS JHU DATA
# ==============================================================================
print("Processing JHU data...")
df_jhu_raw = spark.read.csv("hdfs://localhost:9000/user/alexss/group_project/time_series_covid19_confirmed_global.csv", header=True, inferSchema=True)


# Melt the wide table into long format
jhu_fixed_cols = ["`Province/State`", "`Country/Region`", "`Lat`", "`Long`"]
date_cols = [f"`{c}`" for c in df_jhu_raw.columns if c not in [item.strip('`') for item in jhu_fixed_cols]]

# Create the stack expression for unpivoting
stack_expr = "stack(" + str(len(date_cols)) + ", " + ", ".join([f"'{c.strip('`')}', {c}" for c in date_cols]) + ") as (date, confirmed)"
df_jhu_unpivoted = df_jhu_raw.selectExpr(*jhu_fixed_cols, stack_expr)

# Clean and standardize
df_jhu_std = df_jhu_unpivoted \
    .withColumn("date", to_date(col("date"), "M/d/yy")) \
    .withColumnRenamed("Country/Region", "country") \
    .withColumnRenamed("Province/State", "state") \
    .withColumn("deaths", lit(None).cast("double")) \
    .withColumn("recovered", lit(None).cast("double")) \
    .withColumn("source", lit("JHU")) \
    .select("date", "country", "state", "confirmed", "deaths", "recovered", "source")

# ==============================================================================
# 2. PROCESS OWID DATA
# ==============================================================================
print("Processing OWID data...")
df_owid_raw = spark.read.csv("hdfs://localhost:9000/user/alexss/group_project/owid-covid-data.csv", header=True, inferSchema=True)

df_owid_std = df_owid_raw \
    .withColumn("date", to_date(col("date"))) \
    .withColumnRenamed("location", "country") \
    .withColumnRenamed("total_cases", "confirmed") \
    .withColumnRenamed("total_deaths", "deaths") \
    .withColumn("state", lit(None).cast("string")) \
    .withColumn("recovered", lit(None).cast("double")) \
    .withColumn("source", lit("OWID")) \
    .select("date", "country", "state", "confirmed", "deaths", "recovered", "source")

# ==============================================================================
# 3. PROCESS NYT DATA
# ==============================================================================
print("Processing NYT data...")
df_nyt_raw = spark.read.csv("hdfs://localhost:9000/user/alexss/group_project/nyt_us.csv", header=True, inferSchema=True)

df_nyt_std = df_nyt_raw \
    .withColumn("date", to_date(col("date"))) \
    .withColumnRenamed("cases", "confirmed") \
    .withColumn("country", lit("United States")) \
    .withColumn("state", lit(None).cast("string")) \
    .withColumn("recovered", lit(None).cast("double")) \
    .withColumn("source", lit("NYT")) \
    .select("date", "country", "state", "confirmed", "deaths", "recovered", "source")

# ==============================================================================
# 4. COMBINE AND SAVE THE DATA
# ==============================================================================
print("Combining all data sources...")
df_all = df_jhu_std.union(df_owid_std).union(df_nyt_std)
df_all_sorted = df_all.orderBy("country", "state", "date")

print("Final Combined Data:")
df_all_sorted.show()

print("Saving to Parquet...")
df_all_sorted.write.mode("overwrite").parquet("hdfs://localhost:9000/user/alexss/group_project/covid_cleaned_combined")


print("Processing complete. Output saved to 'covid_cleaned_combined' folder.")

spark.stop()
