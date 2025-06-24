from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,expr
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male","40000"],["Mary","25","Female","30000"]]
emp_schema = "name string,age string, gender string, salary string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
# Case when 
new_gender = emp.withColumn(
    "new_gender",
    when(col("gender") == "Male", "M")
    .when(col("gender") == "Female", "F")
    .otherwise("None")
)
new_gender.show()

#alternate way

new_gender = emp.withColumn(
    "new_gender",
    expr("case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else 'None' end")
)
new_gender.show()