from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male","40000"],["Mary","25","Female","30000"]]
emp_schema = "name string,age string, gender string, salary string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
# withColumn - add a column tax 
emp_tax = emp.withColumn("tax",col("salary")/2)
emp_dropped = emp_tax.drop("salary")
emp_dropped.show()