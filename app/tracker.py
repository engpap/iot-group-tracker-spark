### DOCS ###
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example

### INSTRUCTIONS ###
# conda create --name iot-group-tracker-spark
# conda activate iot-group-tracker-spark
# conda install -c conda-forge pyspark
# python tracker.py

# To run simulation:
# python test/tester.py

# Open a new terminal and run the following command:
# nc -lk 9999

# Spark GUI can be accessed at http://localhost:4040

###### SPECS ######
# The back-end should compute:
# 1. A 1 minute moving average of participants’ age per nationality, computed for each 10 seconds
# ○ For a given nationality, consider all the groups in which at least one participant is of that nationality
# 2. Percentage increase (with respect to the day before) of the 1 minute moving average, for each 10 seconds
# 3. Top nationalities with the highest percentage increase of the 1 minute moving average, for each 10 seconds
####################

###### INPUT ######
# json: {
#   "timestamp": "2021-01-01 00:00:00",
#   "pariticipants": [{"device_id": 1, "nationality": "US", "age": 25}, ... ]
#}

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, window, avg, to_timestamp, expr

class Tracker:
    @staticmethod
    def run(config):
        spark = SparkSession.builder.appName("IoT Tracker Group").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR") # set log level to ERROR to avoid unnecessary logs

        #  schema of the JSON input data
        participant_schema = StructType([
            StructField("device_id", IntegerType(), True),
            StructField("nationality", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        json_schema = StructType([
            StructField("timestamp", StringType(), True),  # Assuming timestamp is a string in the JSON file
            StructField("participants", ArrayType(participant_schema), True)
        ])

        # read the JSON files as a stream
        json_stream = spark.readStream \
            .schema(json_schema) \
            .json(config["input_dir"])

        # explode the participants array to get one row per participant
        exploded_stream = json_stream.select("timestamp", explode("participants").alias("participant"))

        processed_stream = exploded_stream.select(
            to_timestamp("timestamp").alias("timestamp"),
            col("participant.device_id").alias("device_id"),
            col("participant.nationality").alias("nationality"),
            col("participant.age").alias("age")
        )

        # ---- What is WATERMARK --------------------------------
        # Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
        # WATERMARK tells the stream processing system how long to wait for late data before considering a window closed.
        # For instance, if the watermark is set to 5 minutes, the system will wait an additional 5 minutes past the end of a window before finalizing the computations for that window.
        # This allows data that arrives slightly late (up to 5 minutes late in this example) to be included in the analysis.
        # --------------------------------------------------------
        processed_stream = processed_stream.withWatermark("timestamp", config["watermarkDuration"])

        # GOAL: 7 days moving age average of participants for each nationality, computed for each day
        # NOTE: Sorting is NOT supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode; ==> ORDER BY does not work!
        # NOTE: ALIA does not work
        averaged_data = processed_stream.groupBy(
            window("timestamp", config["windowDuration"], config["slideDuration"]),
            "nationality"
        ).avg("age")

        # add a new column with window.start + slide time
        averaged_data = averaged_data.withColumn(
            "window_start_plus_10",
            expr("window.start + INTERVAL 10 SECONDS")
        )

        # join to calculate percentage increase without sorting
        percentage_increase = averaged_data.alias("today").join(
            averaged_data.alias("yesterday"),
            (col("today.window.start") == col("yesterday.window_start_plus_10")) & 
            (col("today.nationality") == col("yesterday.nationality"))
        )

        percentage_increase = percentage_increase.select(
            col("today.nationality"),
            col("yesterday.window.start").alias("prev_start_time"),
            col("yesterday.window.end").alias("prev_end_time"),
            col("yesterday.avg(age)").alias("prev_avg_age"),
            col("today.window.start").alias("start_time"),
            col("today.window.end").alias("end_time"),
            col("today.avg(age)").alias("avg_age"),
            ((col("today.avg(age)") - col("yesterday.avg(age)")) / col("yesterday.avg(age)") * 100).alias("percentage_increase")
        )

        if config["output_format"] == "console": # print to console
            query = percentage_increase.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .start()
        elif config["output_format"] == "parquet": # store files locally => use this for top_n.py
            # NOTE: Data source parquet does not support Complete output mode ==> use Append
            query = percentage_increase.writeStream \
                .outputMode("append") \
                .format("parquet") \
                .option("path", config["output_dir"]) \
                .option("checkpointLocation", config["checkpoint_dir"]) \
                .start()
        else:
            # NOTE: csv is not recommended
            raise ValueError("Invalid output format")

        query.awaitTermination()