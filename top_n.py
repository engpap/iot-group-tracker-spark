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

# NOTE:
# Sorting operations (also rank()) are supported on streaming Datasets only after an aggregation and in Complete Output Mode
# https://stackoverflow.com/questions/64615125/ranking-in-spark-structured-streaming

class TopNationalities:

    def run():
        spark = SparkSession.builder.appName("Top Nationalities").getOrCreate()

        # read the percentage increase data from Parquet files
        percentage_increase_df = spark.read.parquet("output/percentage_increase")

        # add a date column for partitioning
        #percentage_increase_df = percentage_increase_df.withColumn("date", to_date(col("start_time")))
        # # create a window specification to partition by date and order by percentage increase
        # day_window = Window.partitionBy("date").orderBy(desc("percentage_increase"))
        # # rank the nationalities by percentage increase within each day
        # ranked_data = percentage_increase_df.withColumn("rank", row_number().over(day_window))

        # partition by each 10-second window and order by percentage increase
        ten_sec_window = Window.partitionBy(window(col("start_time"), "10 seconds")).orderBy(desc("percentage_increase"))

        # rank the nationalities by percentage increase within each 10-second window
        ranked_data = percentage_increase_df.withColumn("rank", row_number().over(ten_sec_window))

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
        # |prev_start_time    |prev_end_time      |start_time         |end_time           |nationality|rank|
        table.field_names = ["prev_start_time", "prev_end_time", "start_time", "end_time", "nationality", "rank"]


        # Add rows to the table
        for row in all_rows:
            table.add_row([row['prev_start_time'], row['prev_end_time'], row['start_time'], row['end_time'], row['nationality'], row['rank']])

        # store table in a file
        with open("results/top_nationalities.txt", "w") as f:
            f.write(str(table))
