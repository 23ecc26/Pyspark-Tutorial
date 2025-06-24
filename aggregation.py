from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,coalesce
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
data = [("HR", 30000), ("HR", 40000), ("IT", 50000), ("IT", 60000)]
df = spark.createDataFrame(data, ["department", "salary"])
#aggregation - sum,average,count,min,max
new_df = df.groupby("department").agg(sum("salary").alias("total_salary"))
new_df.show()