from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Valorant").getOrCreate()
data = spark.read.csv("player_stats.csv", header=True, inferSchema=True)

processed_data = data.filter(data['kill'] > 10).select('number', 'player')

processed_data.show()

spark.stop()