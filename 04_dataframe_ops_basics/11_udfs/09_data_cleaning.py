# Design a UDF to clean whitespace from a string column and apply it to a DataFrame.
# The UDF should take a string as input and return the string with leading and trailing whitespace removed. The UDF should be registered with Spark so that it can be used in SQL queries as well as in DataFrame transformations. The UDF should handle null values gracefully, returning null if the input string is null. In real-world scenarios, this UDF can be used to clean up text data in a DataFrame, such as removing unnecessary whitespace from user input, log messages, or any other string data that may contain leading or trailing spaces. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex text manipulations, such as filtering rows based on cleaned strings or aggregating data over specific text patterns. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.
# in real-world scenarios, this UDF can be used to clean up text data in a DataFrame, such as removing unnecessary whitespace from user input, log messages, or any other string data that may contain leading or trailing spaces. The UDF can be applied to a DataFrame using the withColumn method or in SQL queries after registering it with Spark. Additionally, the UDF can be used in conjunction with other Spark SQL functions to perform more complex text manipulations, such as filtering rows based on cleaned strings or aggregating data over specific text patterns. The UDF can also be optimized for performance by leveraging Spark's built-in functions and avoiding unnecessary computations.
# this script demonstrates how to create a User Defined Function (UDF) in PySpark to clean whitespace from string columns in a DataFrame. The UDF is designed to handle null values gracefully and can be registered with Spark for use in both SQL queries and DataFrame transformations. The script includes examples of applying the UDF to a DataFrame and shows how it can be used in real-world scenarios for text data cleaning and manipulation.

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create UDF
clean_whitespace_udf = udf(lambda x: x.strip(), StringType())

# Create DataFrame
df = spark.createDataFrame([("  hello  ",), ("  spark  ",)], ["dirty"])

# Apply UDF
df.withColumn('clean', clean_whitespace_udf('dirty')).show()






from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("UDF").getOrCreate()

@udf(StringType())
def clean_whitespace(x):
    return x.strip()

df.withColumn('clean', clean_whitespace('dirty')).show()




