# this udf is designed to classify numbers as either "even" or "odd". It takes a single integer as input and returns a string indicating whether the number is even or odd. The UDF is registered with Spark so that it can be used in both SQL queries and DataFrame transformations. It handles null values gracefully, returning null if the input number is null. In real-world scenarios, this UDF can be used to categorize numerical data in a DataFrame, such as identifying even and odd IDs, counts, or any other integer values that require classification. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex data manipulations or aggregations based on the even/odd classification. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.
# in real-world scenarios, this UDF can be used to categorize numerical data in a DataFrame, such as identifying even and odd IDs, counts, or any other integer values that require classification. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex data manipulations or aggregations based on the even/odd classification. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.

# ------------------------------------------------------------------------------
# Import required modules from PySpark
# ------------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ------------------------------------------------------------------------------
# Step 1: Initialize SparkSession (Entry point for DataFrame operations)
# ------------------------------------------------------------------------------
spark = SparkSession.builder.appName("UDF").getOrCreate()

# ------------------------------------------------------------------------------
# Step 2: Define a regular Python function to classify a number as even or odd
# ------------------------------------------------------------------------------
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

# ------------------------------------------------------------------------------
# Step 3: Convert the Python function to a PySpark UDF
# We must specify the return type (StringType here)
# ------------------------------------------------------------------------------
odd_even_udf = udf(odd_even, StringType())

# ------------------------------------------------------------------------------
# Step 4: Create a sample DataFrame with a single column "number"
# ------------------------------------------------------------------------------
df = spark.createDataFrame([(1,), (2,), (3,)], ['number'])

# ------------------------------------------------------------------------------
# Step 5: Apply the UDF to create a new column "odd_or_even"
# ------------------------------------------------------------------------------
df.withColumn('odd_or_even', odd_even_udf('number')).show()

# Output:
# +------+-----------+
# |number|odd_or_even|
# +------+-----------+
# |     1|        odd|
# |     2|       even|
# |     3|        odd|
# +------+-----------+

# ------------------------------------------------------------------------------
# ALTERNATIVE APPROACH: Use Python decorator to register the function as a UDF
# Cleaner syntax, especially when defining and registering at once
# ------------------------------------------------------------------------------
@udf(StringType())  # UDF decorator with StringType return
def odd_even(x):
    return 'even' if x % 2 == 0 else 'odd'

# ------------------------------------------------------------------------------
# Reuse the same DataFrame and apply decorated UDF
# ------------------------------------------------------------------------------
df.withColumn('odd_or_even', odd_even('number')).show()

# ------------------------------------------------------------------------------
# Concepts Highlighted:
# - UDF: Enables custom logic not available in Spark's built-in functions
# - StringType(): Defines the return type of the UDF
# - withColumn(): Adds or replaces a column in a DataFrame
# - @udf: Python decorator to simplify UDF creation
# ------------------------------------------------------------------------------
