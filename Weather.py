from pyspark.sql import SparkSession
from pyspark.sql.functions import*
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

inputPath = ""
outputPath = ""
inputDF = spark.read.option("header", "true").csv(inputPath)

inputDF.createOrReplaceTempView("inputDF")

df = spark.sql("select ObservationDate, ScreenTemperature, Region from (select ObservationDate,ScreenTemperature,Region,rank() over(order by ScreenTemperature desc) as rnk from inputDF) where rnk = 1")

df.coalesce(1).write.parquet(outputPath)
