from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
emp_data = [["John","30","Male"],["Mary","25","Female"]]
emp_schema = "name string,age string, gender string"
emp = spark.createDataFrame(data=emp_data,schema=emp_schema)
# lit - literals are used to store a static values in dataframe
from pyspark.sql.functions import lit
emp_new = emp.withColumn("First",lit(1)).withColumn("Second",lit("two")).withColumn("Third",lit("three"))
emp_new.show()


## alternate way using dictionaries
new_col = {
    "First" : lit(1),
    "Second" : lit(2),
    "Third" : lit(3)
}
emp_final = emp.withColumns(new_col)
emp_final.show()