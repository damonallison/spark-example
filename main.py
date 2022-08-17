import platform
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions


def print_separator() -> None:
    print("-".join([""] * 80))


def print_environment(spark: SparkSession) -> None:
    """Spark 3.3.0 supports Python 3.7 and above."""
    print_separator()
    print(f"System version: {sys.version}")
    print(f"Python version: {platform.python_version()}")
    print(f"Spark version: {spark.version}")
    print_separator()


if __name__ == "__main__":
    #
    # Spark 2.0 introduced a new entry point called SparkSession, replacing
    # SparkContext, SQLContext and HiveContext as the main Spark entry point.
    #
    with SparkSession.builder.appName("spark-example").getOrCreate() as spark:
        print_environment(spark)

        # NOTE: This "json" file isn't vaid json. Each line contains a JSON object.
        df = spark.read.json("./datasets/people.json.txt")

        df.printSchema()

        # Select and print a few columns
        df.select("name", "age").show()

        # Boolean masking
        df.filter(df["age"] > 18).show()

        # Group
        df.groupBy("age").count().show()

        #
        # SQL
        #

        #
        # Registering a DataFrame as a temporary view allows you to run SQL
        # queries over its data. You can register DFs as session specific or
        # global temp views.
        #
        # Temporary views are session specific. Global temporary views are kept
        # alive until the Spark application terminates.
        #
        # Global temp views are created in a system specific database called
        # "global_temp"
        #
        df.createOrReplaceGlobalTempView("people")

        sqlDf = spark.sql(
            "select * from global_temp.people where age is not null and age > 18"
        )
        sqlDf.printSchema()
        sqlDf.show()
