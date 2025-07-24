from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("playGround").getOrCreate()
#
# data = [
#     (1, 'Vegash', '    Chennai'),
#     (2, 'Edwin', None),
#     (3, 'Jeeva', 'Chennai    '),
#     (4, 'Kathir', '  Madurai'),
#     (5, 'Priya', '        Coimbatore'),
#     (6, 'Arun', None),
#     (7, 'Divya', 'Salem'),
#     (8, 'kavin', 'Madurai'),
#     (9, 'Rani', 'Erode'),
#     (10, 'manoj', 'Chennai')
# ]
#
# df = spark.createDataFrame(data, ['id', 'name', 'location'])
# df.show()
#
# df.select(upper('name')).show()
# df.select(lower('location')).show()
# df.select(initcap('name')).show()
# df.select(trim('location')).show()
# df.select(substring('name',1,3)).show()


data = [
    (1, 'Vegash', 'Chennai 1234' ),
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

dff = spark.createDataFrame(data, ['id', 'name', 'location'])
dff.select(split('location',' ')).show()
dff.select(repeat('id',3)).show()
dff.select(lpad('id',5,'-')).show()
dff.select(regexp_replace('name','Vegash','Vsh').alias('Updated')).show()

dff.select(length('name')).show()
dff.select(length('name').alias('len')).show()