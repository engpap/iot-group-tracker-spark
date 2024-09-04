from pyspark.sql import SparkSession
import os
from prettytable import PrettyTable


class MovingAverage:

    def run():
        spark = SparkSession.builder \
            .appName("ReadParquetExample") \
            .getOrCreate()

        parquet_path = "output/percentage_increase"

        # list all the parquet files in the directory
        parquet_files = []
        for root, dirs, files in os.walk(parquet_path):
            for file in files:
                if file.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, file))

        if parquet_files:
            df = spark.read.parquet(*parquet_files)
            data = df.collect()
            
            table = PrettyTable()
            table.field_names = df.columns

            for row in data:
                table.add_row(row)

            with open("results/queries_output.txt", "w") as f:
                f.write(table.get_string())
            
            print("Data successfully written to output/pretty_table_output.txt")
        else:
            print(f"No Parquet files found in directory: {parquet_path}")

