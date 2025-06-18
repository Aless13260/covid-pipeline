#!/usr/bin/env python3
# covid_analysis_spark.py
# -------------------------------------------------------------
# Batch-only Spark job: reads sample_data.json, produces
# cleaned & aggregated tables ready for downstream dashboards.
# -------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, lag, when, sum as _sum, avg, count, lit
)
from pyspark.sql.window import Window
import os, requests, tempfile, shutil

# ------------------------------------------------------------------
# CONFIG – edit paths if you want something different
# ------------------------------------------------------------------
REMOTE_JSON = ("https://raw.githubusercontent.com/"
               "Aless13260/covid-pipeline/main/sample_data.json")
LOCAL_JSON  = "sample_data.json"          # will download here if absent
OUT_BASE    = "output"                    # HDFS or local FS

# ------------------------------------------------------------------
# 0. Download sample_data.json if not already present
# ------------------------------------------------------------------
if not os.path.exists(LOCAL_JSON):
    print(f"Downloading sample data to {LOCAL_JSON} …")
    with open(LOCAL_JSON, "wb") as f:
        f.write(requests.get(REMOTE_JSON).content)

# ------------------------------------------------------------------
# 1. SparkSession
# ------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("COVID-19 Batch Analysis")
    .getOrCreate()
)

# ------------------------------------------------------------------
# 2. Load JSON into a Spark DataFrame
# ------------------------------------------------------------------
df = spark.read.json(LOCAL_JSON)

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
          _sum("confirmed").alias("cum_confirmed"),
          _sum("deaths")   .alias("cum_deaths")
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
