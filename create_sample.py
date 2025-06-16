# File: export_to_json_lines.py

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Export to JSON Lines") \
        .getOrCreate()

    # The full, real dataset in HDFS
    input_path = "hdfs://localhost:9000/user/alexss/group_project/covid_cleaned_combined"

    # The HDFS directory where Spark will write the intermediate JSON Lines files
    output_path = "hdfs://localhost:9000/user/alexss/group_project/intermediate_json_output"

    print(f"Reading Parquet data from: {input_path}")
    df = spark.read.parquet(input_path)

    # Coalesce to control the number of output files (optional, but good practice)
    # This will create 16 part-files. Adjust as needed.
    df = df.coalesce(16)

    print(f"Writing distributed JSON Lines to: {output_path}")
    df.write.mode("overwrite").json(output_path)

    print("Step 1 complete. Intermediate JSON files have been created in HDFS.")
    spark.stop()