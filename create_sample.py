from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Create Sample").getOrCreate()

    # Read the full dataset from HDFS
    full_df = spark.read.parquet("hdfs://localhost:9000/user/alexss/group_project/covid_cleaned_combined")

    # Take a small sample and consolidate it into a single partition
    sample_df = full_df.limit(1000).coalesce(1)

    # Save it as a single JSON file in your current directory
    sample_df.write.mode("overwrite").json("sample_data_for_team")

    print("Sample data has been created in the 'sample_data_for_team' folder.")
    spark.stop()