from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinOperationsComplete").getOrCreate()

# Load the data into DataFrames
df_breakfast_orders = spark.read.option("header", "true").csv("data/breakfast_orders.txt")
df_token_details = spark.read.option("header", "true").csv("data/token_details.txt")

# Register DataFrames as SQL tables for SQL queries
df_breakfast_orders.createOrReplaceTempView("breakfast_orders")
df_token_details.createOrReplaceTempView("token_details")

# Inner Join
# Showing the result of an inner join between breakfast_orders and token_details based on matching token_color and color
# shows the rows where there is a match in both DataFrames.
inner_join = df_breakfast_orders.join(df_token_details,df_breakfast_orders.token_color == df_token_details.color,"inner")
inner_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders INNER JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Full Outer Join
# A full outer join returns all records when there is a match in either left (breakfast_orders) or right (token_details) DataFrame. If there is no match, the result will contain null values for the missing side.
# shows the result of a full outer join between breakfast_orders and token_details based on matching token_color and color.
full_outer_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "outer")
full_outer_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders FULL OUTER JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Join
# A left join returns all records from the left DataFrame (breakfast_orders) and the matched records from the right DataFrame (token_details). If there is no match, the result will contain null values for the right side.
# shows the result of a left join between breakfast_orders and token_details based on matching token_color and color.
left_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left")
left_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Right Join
# A right join returns all records from the right DataFrame (token_details) and the matched records from the left DataFrame (breakfast_orders). If there is no match, the result will contain null values for the left side.
# shows the result of a right join between breakfast_orders and token_details based on matching token_color
right_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "right")
right_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders RIGHT JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Semi Join
# A left semi join returns only the rows from the left DataFrame (breakfast_orders) that have a match in the right DataFrame (token_details). It does not return any columns from the right DataFrame.
# shows the result of a left semi join between breakfast_orders and token_details based on matching token
left_semi_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left_semi")
left_semi_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT SEMI JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Left Anti Join
# A left anti join returns only the rows from the left DataFrame (breakfast_orders) that do not have a match in the right DataFrame (token_details). It is useful for finding records in one DataFrame that are not present in another.
left_anti_join = df_breakfast_orders.join(df_token_details, df_breakfast_orders.token_color == df_token_details.color, "left_anti")
left_anti_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders LEFT ANTI JOIN token_details ON breakfast_orders.token_color = token_details.color").show()

# Cross Join
# A cross join returns the Cartesian product of the two DataFrames, meaning it combines every row of the left DataFrame (breakfast_orders) with every row of the right DataFrame (token_details). This can result in a large number of rows if both DataFrames are large.
# shows the result of a cross join between breakfast_orders and token_details.
cross_join = df_breakfast_orders.crossJoin(df_token_details)
cross_join.show(50)
# SQL Equivalent
spark.sql("SELECT * FROM breakfast_orders CROSS JOIN token_details").show()

# Stop the Spark session
spark.stop()


# Questions:

# A. Basic Join Operations
#   1. Inner Join: How can you perform an inner join between two DataFrames based on matching 'token_color' in one and 'color' in the other?
#   2. Full Outer Join: What is the method to perform a full outer join between two DataFrames based on the same color matching criteria?

# B. Specific Types of Joins
#   3. Left Join: How do you execute a left join to include all records from the left DataFrame and matched records from the right DataFrame based on color?
#   4. Right Join: Demonstrate how to perform a right join, ensuring all records from the right DataFrame and matched records from the left DataFrame are included.

# C. Less Common Joins
#   5. Left Semi Join: What is a left semi join, and how can it be implemented to include only the rows from the left DataFrame that have corresponding matches in the right DataFrame?
#   6. Left Anti Join: How do you perform a left anti join to obtain only the rows from the left DataFrame that do not have corresponding matches in the right DataFrame?

# D. Unconditional Join
#   7. Cross Join: Describe how to execute a cross join that combines every row of the left DataFrame with every row of the right DataFrame.

# Each question aims to explore different aspects of DataFrame join operations in PySpark, utilizing both the DataFrame API and equivalent SQL queries to demonstrate how these operations can be mirrored in SQL within a Spark environment.

