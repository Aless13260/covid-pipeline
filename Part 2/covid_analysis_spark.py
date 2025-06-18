#!/usr/bin/env python3
# covid_analysis_spark.py
# -------------------------------------------------------------
# Batch-only Spark job: reads sample_data.json, produces
# cleaned & aggregated tables ready for downstream dashboards.
# -------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lag, when, 
    sum as _sum,         # Import 'sum' and rename it to '_sum'
    max as _max,         # Import 'max' and rename it to '_max'
    avg, count, lit
)
from pyspark.sql.window import Window
import os, requests, tempfile, shutil

# ------------------------------------------------------------------
# CONFIG – edit paths if you want something different
# ------------------------------------------------------------------
OUT_BASE    = "/user/alexss/group_project"
# 0. Define the MongoDB connection URI for your input data
mongo_input_uri = "mongodb://localhost:27017/covid_project.cleaned_data"


# ------------------------------------------------------------------
# 1. SparkSession
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("COVID-19 Batch Analysis")
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)

# ------------------------------------------------------------------
# 2. Load JSON from Mongodb into a Spark DataFrame
# ------------------------------------------------------------------
print(f"Reading data from MongoDB URI: {mongo_input_uri}")
df = spark.read.format("mongo") \
    .option("uri", mongo_input_uri) \
    .option("spark.mongodb.input.partitioner", "MongoSinglePartitioner") \
    .load()

# 3. Verify the data was loaded correctly
print("\nSuccessfully loaded data from MongoDB.")
print("Schema:")
df.printSchema()

print("\nSample of the data:")
df.show(10)

# Cast date string → date type
df = df.withColumn("date", to_date(col("date")))

# ------------------------------------------------------------------
# 3. Basic data-quality summary (missing counts / ratios)
# ------------------------------------------------------------------
total_rows = df.count()
missing_exprs = [
    count(when(col(c).isNull(), c)).alias(c) for c in df.columns
]
missing_df = (
    df.agg(*missing_exprs)
      .withColumn("total_rows", lit(total_rows))
)

missing_df.write.mode("overwrite").option("header", True)\
         .csv(f"{OUT_BASE}/missing_summary")

# ------------------------------------------------------------------
# 4. Add daily_new_cases / daily_new_deaths
# ------------------------------------------------------------------
w_country_state = (
    Window.partitionBy("country", "state").orderBy("date")
)

df = (
    df.withColumn("prev_conf",  lag("confirmed").over(w_country_state))
      .withColumn("prev_death", lag("deaths").over(w_country_state))
      .withColumn(
          "daily_new_cases",
          when((col("confirmed") - col("prev_conf")) > 0,
               col("confirmed") - col("prev_conf")).otherwise(0)
      )
      .withColumn(
          "daily_new_deaths",
          when((col("deaths") - col("prev_death")) > 0,
               col("deaths") - col("prev_death")).otherwise(0)
      )
)

# ------------------------------------------------------------------
# 5. Global daily aggregation + 7-day rolling mean + anomaly flag
# ------------------------------------------------------------------
w_rolling7 = (
    Window.partitionBy().orderBy("date").rowsBetween(-6, 0)
)

world_daily = (
    df.groupBy("date")
      .agg(_sum("daily_new_cases").alias("daily_new"))
      .orderBy("date")
      .withColumn("roll_mean", avg("daily_new").over(w_rolling7))
      .withColumn("anomaly",
                  col("daily_new") > col("roll_mean") * 1.5)
)

world_daily.write.mode("overwrite").option("header", True)\
           .csv(f"{OUT_BASE}/world_daily_anomaly")

# ------------------------------------------------------------------
# 6. Per-country daily new cases
# ------------------------------------------------------------------
country_daily = (
    df.groupBy("country", "date")
      .agg(_sum("daily_new_cases").alias("daily_new"))
)

country_daily.write.mode("overwrite").option("header", True)\
             .partitionBy("country")\
             .csv(f"{OUT_BASE}/country_daily")

# ------------------------------------------------------------------
# 7. Latest cumulative confirmed / deaths per country
# ------------------------------------------------------------------
latest = (
    df.orderBy("date")
      .groupBy("country")
      .agg(
          _max("confirmed").alias("cum_confirmed"),
          _max("deaths")   .alias("cum_deaths")
      )
)

latest.write.mode("overwrite").option("header", True)\
      .csv(f"{OUT_BASE}/country_cumulative")

# ------------------------------------------------------------------
# 8. Top-20 countries by confirmed cases (for dashboards)
# ------------------------------------------------------------------
top20 = (
    latest.orderBy(col("cum_confirmed").desc())
          .limit(20)
)

top20.write.mode("overwrite").option("header", True)\
     .csv(f"{OUT_BASE}/top20_confirmed")

# ------------------------------------------------------------------
# 9. Countries & states lists (unique values) – optional
# ------------------------------------------------------------------
countries = (
    df.select("country").distinct()
      .orderBy("country")
)
states = (
    df.select("state").distinct()
      .orderBy("state")
)

countries.write.mode("overwrite").option("header", True)\
        .csv(f"{OUT_BASE}/unique_countries")
states.write.mode("overwrite").option("header", True)\
       .csv(f"{OUT_BASE}/unique_states")

# ------------------------------------------------------------------
# Done
# ------------------------------------------------------------------
print("\n✅  All batch tables written to", OUT_BASE)
spark.stop()
