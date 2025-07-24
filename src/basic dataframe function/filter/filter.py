from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("playGround").getOrCreate()
data = [
    (1,'Vegash','Chennai'),
    (2,'Edwin',None),
    (3,'Jeeva','Chennai'),
    (4,'Kathir','Madurai')
]
df = spark.createDataFrame(data,['id','name','location'])
df.filter(df.name.startswith('V')).show()
df.filter(df.name.isin('Edwin','Vegash')).show()
df.filter(df.location.isNotNull()).show()
df.filter(df.location.isNull()).show()