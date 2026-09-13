from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("check_types").getOrCreate()

trips = spark.read.csv("real_trips.csv", header=True, inferSchema=True)
stations = spark.read.csv("real_stations.csv", header=True, inferSchema=True)

trips = trips.withColumn("start_station_id", col("start_station_id").cast("string"))
trips = trips.withColumn("end_station_id", col("end_station_id").cast("string"))

print("Trips schema after cast:")
trips.printSchema()

spark.stop()