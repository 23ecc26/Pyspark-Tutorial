from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male"],["Mary","25","Female"]]
emp_schema = "name string,age string, gender string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
from pyspark.sql.functions import col,expr
# select-modify the col name as age to emp_age, gender to emp_gender
emp_filtered = emp.select(col("name"),expr("age as emp_age"),expr("gender as emp_gender"))
emp_filtered.show()
