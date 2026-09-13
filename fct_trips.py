from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#from pyspark.sql.functions import count as spark_count

spark = SparkSession.builder.appName("real_data_pipeline").getOrCreate()

trips = spark.read.csv("real_trips.csv", header=True, inferSchema=True)
stations = spark.read.csv("real_stations.csv", header=True, inferSchema=True)

stations_deduped = stations.dropDuplicates(["station_name"])

# Join trips to station names, same left-join logic as fct_trips.sql in Project 1
trips_with_names = trips.join(
    stations_deduped,
    trips.start_station_name == stations_deduped.station_name,
    "left"
)

# Top 10 busiest departure stations
top_stations = trips_with_names.groupBy("start_station_name") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(10)

print("Top 10 busiest departure stations:")
top_stations.show(truncate=False)

unmatched = trips_with_names.filter(col("station_name").isNull()).count()
total = trips_with_names.count()
print(f"{unmatched} out of {total} trips reference a station not in the current station list")

#check dubicates in stations table
# duplicate_names = stations.groupBy("station_name") \
#     .agg(spark_count("*").alias("num_ids")) \
#     .filter(col("num_ids") > 1)

# print("Station names with more than one station_id:")
# duplicate_names.show(truncate=False)

# Keep the session alive so you can check the Spark UI before it shuts down
input("Press Enter to stop Spark (check localhost:4040 in your browser first)...")

spark.stop()