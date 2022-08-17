from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession.builder.appName("spark-example").getOrCreate()

df = spark.read.text("README.md")

# Filter creates a new df with the given predicate
# TODO: how do predicates work?
num_spark = df.filter(df.value.contains("spark")).count()
num_example = df.filter(df.value.contains("example")).count()

print(f"Totla lines {df.count()}")
print(f"Lines w/ spark {num_spark}")
print(f"Lines w/ example {num_example}")

print(f"Lines with spark: {df.filter(df.value.contains('spark')).collect()}")
spark.stop()
