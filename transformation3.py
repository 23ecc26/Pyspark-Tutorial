from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male"],["Mary","20","Female"]]
emp_schema = "name string,age string, gender string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
# where - filter the employees whose age are greater than 30
emp_filtered = emp.select("name","age","gender").where("age > 20")