from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.appName("airflow_citibike").getOrCreate()

    trips = spark.read.csv("/opt/airflow/data/real_trips.csv", header=True, inferSchema=True)
    stations = spark.read.csv("/opt/airflow/data/real_stations.csv", header=True, inferSchema=True)
    stations_deduped = stations.dropDuplicates(["station_name"])

    trips_with_names = trips.join(
        stations_deduped,
        trips.start_station_name == stations_deduped.station_name,
        "left",
    )

    top_stations = (
        trips_with_names.groupBy("start_station_name")
        .count()
        .orderBy(col("count").desc())
        .limit(10)
    )

    for row in top_stations.collect():
        print(f"{row['start_station_name']}: {row['count']} trips")

    spark.stop()


if __name__ == "__main__":
    main()