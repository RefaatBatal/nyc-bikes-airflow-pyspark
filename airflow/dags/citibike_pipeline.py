from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = (
        SparkSession.builder
        .appName("airflow_citibike")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )
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
    results = top_stations.collect()

    for row in results:
        print(f"{row['start_station_name']}: {row['count']} trips")

    total_trips = trips.count()
    unmatched = trips_with_names.filter(col("station_name").isNull()).count()

    spark.stop()

    return {
        "total_trips_processed": total_trips,
        "unmatched_station_trips": unmatched,
        "top_station": results[0]["start_station_name"] if results else None,
        "top_station_count": results[0]["count"] if results else None,
    }

if __name__ == "__main__":
    main()