from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("setup_check").getOrCreate()
print(spark.range(5).collect())
spark.stop()