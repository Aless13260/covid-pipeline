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

print("SparkSession created. Starting data download and processing...")

# ==============================================================================
# 2. DOWNLOAD DATA: Fetch files from the web
# ==============================================================================
# JHU, OWID, and NYT URLs
jhu_base_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/"
jhu_files = [
    "time_series_covid19_confirmed_global.csv",
]
owid_url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
nyt_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"

# Define local file paths
path_confirmed = "time_series_covid19_confirmed_global.csv"
path_owid = "owid-covid-data.csv"
path_nyt = "nyt_us.csv"

# Download the files
print("Downloading JHU data...")
for f in jhu_files:
    r = requests.get(jhu_base_url + f)
    with open(f, 'wb') as fp:
        fp.write(r.content)

print("Downloading OWID data...")
r = requests.get(owid_url)
with open(path_owid, 'wb') as fp:
    fp.write(r.content)

print("Downloading NYT data...")
r = requests.get(nyt_url)
with open(path_nyt, 'wb') as fp:
    fp.write(r.content)

print("All files downloaded.")

# ==============================================================================
# 3. PROCESS JHU DATA
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
# 4. PROCESS OWID DATA
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
# 5. PROCESS NYT DATA
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
# 6. COMBINE AND SAVE THE DATA
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
