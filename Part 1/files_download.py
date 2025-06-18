# ==============================================================================
# 1. SETUP: Import libraries and create the SparkSession
# ==============================================================================
import requests
from pyspark.sql import SparkSession


# This is the entry point for any Spark application.
spark = SparkSession.builder \
    .appName("CovidDataCombination") \
    .getOrCreate()

print("SparkSession created. Starting data download and processing...")

# ==============================================================================
# 1. DOWNLOAD DATA: Fetch files from the web
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
