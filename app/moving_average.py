from pyspark.sql import SparkSession
import os
from prettytable import PrettyTable
from config import CONFIG
import time


class MovingAverage:

    def run():
        spark = SparkSession.builder \
            .appName("ReadParquetExample") \
            .getOrCreate()

        parquet_path = CONFIG["output_dir"]

        # list all the parquet files in the directory
        parquet_files = []
        for root, dirs, files in os.walk(parquet_path):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))

        if parquet_files:
            df = spark.read.parquet(*parquet_files)
            df = df.orderBy("prev_start_time", "start_time", "nationality")
            data = df.collect()
            
            table = PrettyTable()
            table.field_names = df.columns

            for row in data:
                table.add_row(row)

            filename = CONFIG["results_dir"] + "/moving_avg_" + str(time.time())
            with open(filename, "w") as f:
                f.write(table.get_string())
            
            print(f"\n\n\nMoving average result successfully written to: {filename}")
        else:
            print(f"\n\n\nNo Parquet files found in directory: {parquet_path}")

