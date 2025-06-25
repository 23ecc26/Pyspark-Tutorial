from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ParquetToCSV").getOrCreate()
df = spark.read.parquet("mtcars.parquet").load("C:\Users\SOFIA SELCY\OneDrive\Desktop\Pyspark Tutorial")
df.write.csv("mtcars_csv_output", header=True, mode="overwrite")
