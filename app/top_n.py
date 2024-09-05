'''
This script:
- reads precentage increase data from the Parquet files
- partitions the data by each 10-second window
- orders the data by percentage increase
- ranks (attach a index positon, e.g. 1,2,3...) the nationalities by percentage increase within each 10-second window
- filters to get top 3 nationalities per day
- prints out only window and nationality
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, desc, window
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from prettytable import PrettyTable
from config import CONFIG
import time

# NOTE:
# Sorting operations (also rank()) are supported on streaming Datasets only after an aggregation and in Complete Output Mode
# https://stackoverflow.com/questions/64615125/ranking-in-spark-structured-streaming

class TopNationalities:

    def run():
        spark = SparkSession.builder.appName("Top Nationalities").getOrCreate()

        # read the percentage increase data from Parquet files
        percentage_increase_df = spark.read.parquet(CONFIG["output_dir"])

        # partition by each slide duration (e.g. 10 seconds) and order by percentage increase
        slided_window = Window.partitionBy(window(col("start_time"), CONFIG["slideDuration"])).orderBy(desc("percentage_increase"))

        # rank the nationalities by percentage increase within each 10-second window
        ranked_data = percentage_increase_df.withColumn("rank", row_number().over(slided_window))

        # filter to get the top nationalities per day
        top_nationalities = ranked_data.filter(col("rank") <= 3)

        # print out only window and nationality
        result_df = top_nationalities.select(
            col("prev_start_time"),
            col("prev_end_time"),
            col("start_time"),
            col("end_time"),
            col("nationality"),
            col("rank"),
        )

        # result_df.show(truncate=False) THIS PRINTS OUT ONLY 20 ROWS; CANNOT BE SET TO INFINITY
        # WORKAROUND: collect all rows and print them
        all_rows = result_df.collect()
        table = PrettyTable()
        table.field_names = ["prev_start_time", "prev_end_time", "start_time", "end_time", "nationality", "rank"]
        for row in all_rows:
            table.add_row([row['prev_start_time'], row['prev_end_time'], row['start_time'], row['end_time'], row['nationality'], row['rank']])

        # store table in a file
        filename = CONFIG["results_dir"] + "/top_nationalities_" + str(time.time())
        with open(filename, "w") as f:
            f.write(str(table))
        print(f"\n\n\nITTop Nationalities result successfully written to: {filename}")