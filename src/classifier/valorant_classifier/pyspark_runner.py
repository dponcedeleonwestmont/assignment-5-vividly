from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Valorant").getOrCreate()
data = spark.read.csv("../../player_stats2.csv", header=True, inferSchema=True)

processed_data = data.filter(data['kill'] > 20).select('number', 'player', 'map')

processed_data.show()

spark.stop()