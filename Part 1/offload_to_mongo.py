# File: offload_to_mongo.py

from pyspark.sql import SparkSession

if __name__ == "__main__":

    # This script is ONLY for moving existing data from HDFS to MongoDB.

    spark = SparkSession.builder \
        .appName("HDFS to MongoDB Offload") \
        .getOrCreate()

    # --- POINT TO YOUR EXISTING HDFS DATA ---
    # Replace this with the HDFS path where your Parquet files are already saved.
    hdfs_input_path = "hdfs://localhost:9000/user/alexss/group_project/covid_cleaned_combined"
    
    print(f"Reading existing Parquet data from: {hdfs_input_path}")
    df = spark.read.parquet(hdfs_input_path)

    # --- CONFIGURE THE MONGODB DESTINATION ---
    # Replace this with your MongoDB connection details.
    mongo_output_uri = "mongodb://localhost:27017/covid_project.cleaned_data"
    
    print(f"Writing data to MongoDB at: {mongo_output_uri}")
    
    # --- START THE COPY ---
    # This writes the DataFrame loaded from HDFS into MongoDB.
    df.write.format("mongo") \
        .mode("overwrite") \
        .option("uri", mongo_output_uri) \
        .save()
        
    print("Offload complete! Data has been copied to MongoDB.")

    spark.stop()