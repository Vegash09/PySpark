from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("playGround").getOrCreate()
data = [
    (1,'Vegash','Chennai'),
    (1,'Edwin','Bangalore'),
    (3,'Jeeva','Chennai'),
    (4,'Kathir','Madurai')
]
df = spark.createDataFrame(data,['id','name','location'])
df.select(expr('id * 100').alias('NewId')).show()
df.select(col('name').alias('Name')).show()
df.selectExpr('id','id*100 as UpdatedId').show()




