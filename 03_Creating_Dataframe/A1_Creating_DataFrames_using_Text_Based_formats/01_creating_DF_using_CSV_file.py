# Create a DataFrame from employee.txt (csv) file by implicitly providing Schema
# Importing the necessary libraries
from pyspark.sql import SparkSession

# Creating a Spark session
# The Spark session is the entry point to programming with Spark with the Dataset and DataFrame API.
# The Spark session allows you to create DataFrames, register DataFrames as tables, execute SQL over tables, cache tables, and read parquet files.
spark = SparkSession.builder \
    .appName("Employee Data Analysis") \
    .getOrCreate()

# Read the employee data from a CSV file into a DataFrame
# Replace '<path to employee.csv>' with the actual path to your CSV file


employeeDF = spark.read.option("inferSchema", "true").\
    option("header", "true").csv(r"data/employee.txt")


# The 'inferSchema' option is set to 'true' to automatically infer the data types of the columns in the CSV file.
# The 'header' option is set to 'true' to indicate that the first row of the CSV file contains the column names.
# the option is a method that allows you to specify various options for reading the CSV file, such as the delimiter, quote character, and more. In this case, we are using it to specify that the first row contains headers and to infer the schema of the DataFrame.
# The 'csv' method is used to read the CSV file and create a DataFrame from it. The path to the CSV file is provided as an argument to this method.

# Print the inferred schema of the DataFrame to show the data types and structure
employeeDF.printSchema()

# Display the contents of the DataFrame to provide a snapshot of the data
employeeDF.show()

# Stop the Spark session
spark.stop()