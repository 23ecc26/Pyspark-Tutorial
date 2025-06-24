from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male"],["Mary","25","Female"]]
emp_schema = "name string,age string, gender string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
#selectExpr - modify the name as emp_name, cast(age) string to int
emp_casted = emp.selectExpr("name as emp_name","cast(age as int) as age","gender")
emp_casted.show()