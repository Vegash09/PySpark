from pycparser.ply.cpp import literals
from pyspark.sql import *
from pyspark.sql.functions import col,lit,regexp_replace
spark = SparkSession.builder.appName("playGround").getOrCreate()
data = [
    (1,'Vegash','Chennai'),
    (2,'Edwin',None),
    (3,'Jeeva','Chennai'),
    (4,'Kathir','Madurai')
]
df = spark.createDataFrame(data,['id','name','location'])
#df.show()
# df.where("id == 1 AND location = 'Chennai'").show()
# df.where("location like 'C%'").show()

df = df.withColumn('literals', lit('literals'))
df = df.withColumn('literals', regexp_replace('literals', 'literals', 'lit'))
df.where("(id*100)>200").df.show()
df = df.where("location IS NOT NULL")
df.show()


