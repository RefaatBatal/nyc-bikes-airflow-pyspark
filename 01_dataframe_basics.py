from pyspark.sql import SparkSession
# SparkSession is the single connection to the entire spark engine;
# every DataFrame, every read, every computation happens through this one object.
# Nothing in PySpark works without one existing first; 
# it's the literal starting point of every script, which is why it's always the very first thing you create.
from pyspark.sql import functions as F
# What it actually is: a module — literally a file full of pre-written functions — 
# that ships with PySpark, containing dozens of ready-made operations for exactly the things SQL can do: 
# count, avg, sum, max, date manipulation, string manipulation, and more.

spark = SparkSession.builder.appName("dataframe_basics").getOrCreate()

df = spark.read.csv("sample_trips.csv", header=True, inferSchema=True)

df.printSchema()
df.show()

subscribers = df.filter(df.user_type == "Subscriber")
print("Only subscriber trips:")
subscribers.show()

short_trips = df.select("trip_id", "duration_minutes").filter(df.duration_minutes < 10)
print("Short trips only, two columns:")
short_trips.show()

trip_summary = df.groupBy("user_type").agg(
    F.count("trip_id").alias("num_trips"),
    F.avg("duration_minutes").alias("avg_duration")
)
print("Trip counts and average duration by user type:")
trip_summary.show()

stations_df = spark.read.csv("sample_stations.csv", header=True, inferSchema=True)

trips_with_borough = df.join(
    stations_df,
    df.start_station == stations_df.station_name,
    "left"
)
print("Trips joined with station info:")
trips_with_borough.select("trip_id", "start_station", "borough").show()

spark.stop() 