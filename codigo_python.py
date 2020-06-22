from pyspark.sql import SparkSession

appName = "Converter csv para parquet"
master = "local"

spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

df = spark.read.format("csv").option("header", "true").load("/FileStore/tables/load.csv")

df = df.withColumn("id", df.id.cast('int'))
df = df.withColumn("age", df.age.cast('int'))
df = df.withColumn("create_date", df.create_date.cast('timestamp'))
df = df.withColumn("update_date", df.update_date.cast('timestamp'))

df.write.parquet("arquivo.parquet")