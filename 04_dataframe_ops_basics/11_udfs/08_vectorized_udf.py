# Create a Pandas UDF that squares each number in a column.
# The UDF should take a single column of integers as input and return a new column with each integer squared. The UDF should be registered with Spark so that it can be used in SQL queries as well as in DataFrame transformations. The UDF should handle null values gracefully, returning null if the input value is null. In real-world scenarios, this UDF can be used to perform mathematical operations on numerical data in a DataFrame, such as calculating squares of measurements, scores, or any other numerical values that require squaring. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex mathematical manipulations or aggregations on numerical data. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.
# this is a Pandas UDF that squares each number in a column. The UDF takes a single column of integers as input and returns a new column with each integer squared. It is registered with Spark for use in SQL queries and DataFrame transformations, handling null values gracefully. This UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registration, and can be used in conjunction with other Spark SQL functions for complex mathematical manipulations or aggregations on numerical data.
# in real life scenarios, this UDF can be used to perform mathematical operations on numerical data in a DataFrame, such as calculating squares of measurements, scores, or any other numerical values that require squaring. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex mathematical manipulations or aggregations on numerical data. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.

from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, PandasUDFType

spark = SparkSession.builder.appName("UDF").getOrCreate()
# Create Pandas UDF
@pandas_udf("integer", PandasUDFType.SCALAR)
def square(x):
    return x * x

# Create DataFrame
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# Apply UDF
df.withColumn('squared', square('number')).show()






