### DOCS ###
# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#quick-example

### INSTRUCTIONS ###
# conda create --name iot-group-tracker-spark
# conda activate iot-group-tracker-spark
# conda install -c conda-forge pyspark
# python tracker.py

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

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, window, avg, to_timestamp

CONFIG = {
    "windowDuration": "1 week", #1 minute",
    "slideDuration": "1 day", #10 seconds",
}

spark = SparkSession.builder.appName("IoT Tracker Group").getOrCreate()

# set log level to ERROR to avoid unnecessary logs
spark.sparkContext.setLogLevel("ERROR")

# df representing the stream of input lines from connection to localhost:9999
input = spark.readStream.format("socket").option("host", "localhost").option("port", 9998).load()
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

# 1. 7 days moving age average of participants for each nationality, computed for each day
averaged_data = parsed_data.groupBy(
    window("timestamp", CONFIG["windowDuration"], CONFIG["slideDuration"]),
    "nationality"
).avg("age").orderBy("window", "nationality") # sort by window and nationality for better visualization in console

query = averaged_data.writeStream.outputMode("complete").format("console").option("truncate", False).start()

query.awaitTermination()
