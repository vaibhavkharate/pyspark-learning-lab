# Join Operations in PySpark
# join operations are essential for combining data from multiple DataFrames based on common keys. In this exercise, we will explore various join types using the employees and departments datasets in PySpark.
# IN REAL SCENARIOS, YOU MAY WANT TO COMBINE DATA FROM DIFFERENT SOURCES TO GAIN INSIGHTS. THIS EXERCISE DEMONSTRATES HOW TO PERFORM JOIN OPERATIONS USING THE DataFrame API AND SQL QUERIES IN PySpark.
# use of join in real scenarios, you may want to combine data from different sources to gain insights. This exercise demonstrates how to perform join operations using the DataFrame API and SQL queries in PySpark.


from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("JoinOperationsDemo").getOrCreate()

# Load the data into DataFrames
df_employees = spark.read.csv("data/Employees.csv", header=True, inferSchema=True)
df_departments = spark.read.csv("data/Departments.csv", header=True, inferSchema=True)

# Register DataFrames as SQL tables
df_employees.createOrReplaceTempView("employees")
df_departments.createOrReplaceTempView("departments")

# Inner Join
inner_join = df_employees.join(df_departments, "DepartmentID", "inner")
inner_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees INNER JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Outer (Full) Join
full_outer_join = df_employees.join(df_departments, "DepartmentID", "outer")
full_outer_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees FULL OUTER JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Join
left_join = df_employees.join(df_departments, "DepartmentID", "left")
left_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Right Join
right_join = df_employees.join(df_departments, "DepartmentID", "right")
right_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees RIGHT JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Anti Join
# A left anti join returns only the rows from the left DataFrame (employees) that do not have a matching row in the right DataFrame (departments). This is useful for identifying records in one dataset that are not present in another.
# the left anti join is implemented using the `join` method with the "left_anti" join type. In this case, we are looking for employees who do not belong to any department listed in the departments DataFrame.
# in real life scenarios, left anti joins can be used to find orphan records, such as employees without a department assignment, which can help in data cleaning and validation processes.
# the left anti join keeps only the rows from the left DataFrame (employees) that do not have a corresponding match in the right DataFrame (departments). This is particularly useful for identifying records that are unique to one dataset, allowing for better data analysis and decision-making.
left_anti_join = df_employees.join(df_departments, "DepartmentID", "left_anti")
left_anti_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT ANTI JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Left Semi Join
# A left semi join returns only the rows from the left DataFrame (employees) that have a matching row in the right DataFrame (departments). This is useful for filtering records in one dataset based on the existence of related records in another dataset.
# the left semi join is implemented using the `join` method with the "left_semi
# join" join type. In this case, we are looking for employees who belong to departments listed in the departments DataFrame.
# in real life scenarios, left semi joins can be used to filter records based on the existence
# of related records in another dataset, such as finding employees who are assigned to valid departments, which can help in data validation and integrity checks.
# the left semi join keeps only the rows from the left DataFrame (employees) that have a corresponding match in the right DataFrame (departments). This is particularly useful for filtering records based on the existence of related records, allowing for more targeted data analysis and decision-making.
left_semi_join = df_employees.join(df_departments, "DepartmentID", "left_semi")
left_semi_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees LEFT SEMI JOIN departments ON employees.DepartmentID = departments.DepartmentID").show()

# Self Join (demonstrating employees from the same department)
# A self join is a join operation where a DataFrame is joined with itself. This is useful for comparing rows within the same dataset, such as finding employees who work in the same department.
# the self join is implemented by creating two aliases of the employees DataFrame and joining them on the "DepartmentID" column. This allows us to find pairs of employees who belong to the same department.
# in real life scenarios, self joins can be used to analyze relationships within the same dataset, such as identifying colleagues working in the same department, which can help in team management and collaboration analysis.
# the self join allows us to compare rows within the same DataFrame, enabling us to find relationships and patterns among records that share common attributes, such as employees working in the same department. This can provide valuable insights for organizational analysis and decision-making.
self_join = df_employees.alias("emp1").join(df_employees.alias("emp2"),
                                            col("emp1.DepartmentID") == col("emp2.DepartmentID"),
                                            "inner")
self_join.show()
# SQL Equivalent
spark.sql("""
SELECT emp1.*, emp2.*
FROM employees emp1
INNER JOIN employees emp2 ON emp1.DepartmentID = emp2.DepartmentID
""").show()

# Cross Join
# A cross join returns the Cartesian product of two DataFrames, meaning it combines every row of the first DataFrame with every row of the second DataFrame. This is useful for generating combinations of records from two datasets.
# the cross join is implemented using the `crossJoin` method. In this case, we
# are combining every employee with every department, regardless of any matching condition. This can be useful for scenarios where you want to explore all possible combinations of two datasets.
# in real life scenarios, cross joins can be used for generating combinations of records, such as pairing employees with all departments for analysis or testing purposes. However, caution should be exercised when using cross joins, as they can result in a large number of records and may lead to performance issues if the datasets are large.
# the cross joins shows the Cartesian product of two DataFrames, allowing for the exploration of all possible combinations of records from both datasets. This can be particularly useful for generating test cases, simulations, or analyzing potential relationships between two sets of data.
cross_join = df_employees.crossJoin(df_departments)
cross_join.show()
# SQL Equivalent
spark.sql("SELECT * FROM employees CROSS JOIN departments").show()

# Stop the Spark session
spark.stop()

# Questions:

# A. Basic Join Types
#   1. Inner Join: How can you perform an inner join between the employees and departments DataFrames based on the "DepartmentID"?
#   2. Outer (Full) Join: What is the method to perform a full outer join on the "DepartmentID" between employees and departments?

# B. Specific Join Scenarios
#   3. Left Join: Demonstrate how to execute a left join between the employees and departments DataFrames, including all records from employees.
#   4. Right Join: How do you perform a right join, ensuring all records from the departments DataFrame are included even if there are no matches in employees?

# C. Specialized Join Types
#   5. Left Anti Join: What is a left anti join, and how can it be implemented to get the records from employees that do not match any department?
#   6. Left Semi Join: Explain a left semi join, which includes only the rows from employees that have a corresponding match in departments.

# D. Additional Join Operations
#   7. Self Join: How would you demonstrate a self join within the employees DataFrame to show employees from the same department?
#   8. Cross Join: Describe how to execute a cross join that combines every row of employees with every row of departments, disregarding any matching condition.

# Each question targets understanding different aspects of DataFrame join operations in PySpark,
# using both the DataFrame API and equivalent SQL queries to demonstrate how these operations can be achieved in SQL within a Spark environment.

