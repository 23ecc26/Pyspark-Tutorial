from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,coalesce
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male","40000"],["Mary","25","Female","30000"],["Mariya","20","null","50000"]]
emp_schema = "name string,age string, gender string, salary string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
# repartition
emp.rdd.getNumPartitions()
emp_partitioned = emp.repartition(4)
emp_partitioned.rdd.getNumPartitions()