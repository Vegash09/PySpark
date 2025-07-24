from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Sample Data
data = [
    (1, 'Vegash', 'Chennai 1234'),
    (2, 'Edwin', None),
    (3, 'Jeeva', 'Chennai 1234'),
    (4, 'Kathir', 'Madurai 1234'),
    (5, 'Priya', 'Coimbatore 1234'),
    (6, 'Arun', None),
    (7, 'Divya', 'Salem 1234'),
    (8, 'kavin', 'Madurai 1234'),
    (9, 'Rani', 'Erode 1234'),
    (10, 'manoj', 'Chennai 1234')
]

# Create DataFrame
dff = spark.createDataFrame(data, ['id', 'name', 'location'])


dff.select(sum("id").alias("SUM")).show()
dff.select(avg("id").alias("AVG")).show()
dff.select(min("id").alias("MIN")).show()
dff.select(max("id").alias("MAX")).show()
dff.select(round(avg("id"), 2).alias("Rounded AVG")).show()
from pyspark.sql.functions import col
dff.select(abs(col("id") * -1).alias("ABS")).show()
