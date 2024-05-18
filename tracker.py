### DOCS ###
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example

### INSTRUCTIONS ###
# conda create --name iot-group-tracker-spark
# conda activate iot-group-tracker-spark
# conda install -c conda-forge pyspark
# python tracker.py

# To run simulation:
# python tester.py

# Open a new terminal and run the following command:
# nc -lk 9999

# Spark GUI can be accessed at http://localhost:4040

###### SPECS ######
# The back-end should compute:
# 1. A 1 minute moving average of participants’ age per nationality, computed for each 10 seconds
# ○ For a given nationality, consider all the groups in which at least one participant is of that nationality
# 2. Percentage increase (with respect to the day before) of the 1 minute moving average, for each 10 seconds
# 3. Top nationalities with the highest percentage increase of the 1 minute moving average, for each day
####################

###### INPUT ######
# json: {
#   "timestamp": "2021-01-01 00:00:00",
#   "pariticipants": [{"device_id": 1, "nationality": "US", "age": 25}, ... ]
#}

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, window, avg, to_timestamp

# CONFIG = {
#     "windowDuration": "1 week",
#     "slideDuration": "1 day",
#     "watermarkDuration": "1 day",
# }

CONFIG = {
    "windowDuration": "1 minute",
    "slideDuration": "10 seconds",
    "watermarkDuration": "5 seconds",
}


spark = SparkSession.builder.appName("IoT Tracker Group").getOrCreate()

# set log level to ERROR to avoid unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# df representing the stream of input lines from connection to localhost:9999
input = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
print("(DEBUG) input: ", input)

device_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("action", StringType(), True),
    StructField("group_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("age", IntegerType(), True),
])

# Assuming data is received as a comma-separated string
# examples:
# "2021-01-01 00:00:00,add,1,1,US,25"
# "2021-01-01 00:00:00,remove,1,2,US,30"
def parse_data(data_str):
    fields = split(data_str, ",")
    return {
        "timestamp": fields[0],
        "action": fields[1],
        "group_id": fields[2],
        "device_id": fields[3],
        "nationality": fields[4],
        "age": fields[5].cast("integer"),
    }

# explode takes an array as input and produces a new row for each element in the array
# alias is used to rename the column

parsed_data = input.select(explode(split(input.value, "\n")).alias("data")) \
    .select(split(col("data"), ",").alias("data")).select(
    col("data").getItem(0).cast("timestamp").alias("timestamp"),
    col("data").getItem(1).alias("action"),
    col("data").getItem(2).alias("group_id"),
    col("data").getItem(3).alias("device_id"),
    col("data").getItem(4).alias("nationality"),
    col("data").getItem(5).cast("integer").alias("age"),
)

# Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark;
# WATERMARK tells the stream processing system how long to wait for late data before considering a window closed.
# For instance, if the watermark is set to 5 minutes, the system will wait an additional 5 minutes past the end of a window 
# before finalizing the computations for that window.
# This allows data that arrives slightly late (up to 5 minutes late in this example) to be included in the analysis.
parsed_data = parsed_data.withWatermark("timestamp", CONFIG["watermarkDuration"])

# 1. 7 days moving age average of participants for each nationality, computed for each day
# Sorting is not supported on streaming DataFrames/Datasets, unless it is on aggregated DataFrame/Dataset in Complete output mode; ==> NO ORDER BY
# Don't use alias, does not work
averaged_data = parsed_data.groupBy(
    window("timestamp", CONFIG["windowDuration"], CONFIG["slideDuration"]),
    "nationality"
).avg("age") #.orderBy("window", "nationality") # sort by window and nationality for better visualization in console

#averaged_data = averaged_data.withColumn("window_start", to_timestamp("window.start")).withColumn("window_end", to_timestamp("window.end"))

# Join to calculate percentage increase without sorting
percentage_increase = averaged_data.alias("today").join(
    averaged_data.alias("yesterday"),
    (col("today.window.start") == col("yesterday.window.end")) & 
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

# Define the streaming query, using append mode without sorting
query = percentage_increase.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()






##### OLD CODE #####
# 2. Percentage increase (with respect to the day before) of the 1 minute moving average, for each 10 seconds
# AnalysisException: Join between two streaming DataFrames/Datasets is not supported in Complete output mode, only in Append output mode;
# WORKS
# percentage_increase = averaged_data.join(
#     averaged_data.withColumnRenamed("avg(age)", "avg_age_yesterday").withColumnRenamed("window", "window_yesterday"),
#     col("window.start") == col("window_yesterday.end") \
#     ).withColumn("percentage_increase", (col("avg(age)") - col("avg_age_yesterday")) / col("avg_age_yesterday") * 100)

# truncate: false => show full column data
#query = percentage_increase.writeStream.outputMode("append").format("console").option("truncate", False).start()
#query = averaged_data.writeStream.format("csv").option("checkpointLocation", "/Users/dre/Dev/iot-group-tracker-spark/").option("path", "/Users/dre/Dev/iot-group-tracker-spark/").start()