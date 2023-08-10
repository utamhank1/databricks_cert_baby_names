# Databricks notebook source
# MAGIC %python
# MAGIC %pip install black tokenize-rt

# COMMAND ----------

# MAGIC %md # SA coding assessment: Data Engineering, Baby Names
# MAGIC ## Version 2022.02
# MAGIC
# MAGIC What you'll do:
# MAGIC * We provide the dataset. You will load it into dataframes, and perform some data cleansing and transformation tasks.
# MAGIC * You will answer a series of questions to show insights from the data.
# MAGIC * There are also some written-answer questions.
# MAGIC
# MAGIC *We care about the process, not the result.*  I.e., we're looking for proper use of data engineering techniques and understanding of the code you've written.  
# MAGIC
# MAGIC This Data Engineering section is scored out of 50 points.

# COMMAND ----------

# DBTITLE 1,Setup Env
# This folder is for you to write any data as needed. Write access is restricted elsewhere. You can always read from dbfs.
aws_role_id = "AROAUQVMTFU2DCVUR57M2"
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
userhome = f"s3a://e2-interview-user-data/home/{aws_role_id}:{user}"
print(userhome)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Baby Names Data Set
# MAGIC
# MAGIC This dataset comes from a website referenced by [Data.gov](http://catalog.data.gov/dataset/baby-names-beginning-2007). It lists baby names used in the state of NY from 2007 to 2018.
# MAGIC
# MAGIC Run the following two cells to copy this file to a usable location.

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC import java.net.URL
# MAGIC import java.io.File
# MAGIC import org.apache.commons.io.FileUtils
# MAGIC
# MAGIC val tmpFile = new File("/tmp/rows.json")
# MAGIC FileUtils.copyURLToFile(new URL("https://health.data.ny.gov/api/views/jxy9-yhdk/rows.json?accessType=DOWNLOAD"), tmpFile)

# COMMAND ----------

# https://docs.python.org/3/library/hashlib.html#blake2
from hashlib import blake2b

user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
h = blake2b(digest_size=4)
h.update(user.encode("utf-8"))
display_name = "user_" + h.hexdigest()
print("Display Name: " + display_name)

dbutils.fs.cp('file:/tmp/rows.json', userhome + '/rows.json')
dbutils.fs.cp(userhome + '/rows.json' ,f"dbfs:/tmp/{display_name}/rows.json")
baby_names_path = f"dbfs:/tmp/{display_name}/rows.json"

print("Baby Names Path: " + baby_names_path)
dbutils.fs.head(baby_names_path)

# Ensure you use baby_names_path to answer the questions. A bug in Spark 2.X will cause your read to fail if you read the file from userhome. 
# Please note that dbfs:/tmp is cleaned up daily at 6AM pacific

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Baby Names Question 1 - Nested Data [15 Points]
# MAGIC
# MAGIC
# MAGIC Use Spark SQL's native JSON support to read the baby names file into a dataframe. Use this dataframe to create a temporary table containing all the nested data columns ("sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count") so that they can be queried using SQL. 
# MAGIC
# MAGIC Hint: you can use ```dbutils.fs.head(baby_names_path)``` to take a look at the dataset before reading it in. 
# MAGIC
# MAGIC Suggested Steps:
# MAGIC 1. Read in the JSON data
# MAGIC 2. Pull all columns in the nested data column to top level, following the schema specified above. There are [built-in Spark SQL functions](https://spark.apache.org/docs/latest/api/sql/index.html) that will accomplish this.
# MAGIC 3. Create a temp table from this expanded dataframe using createOrReplaceTempView()
# MAGIC

# COMMAND ----------

# Helper function for question 1.

def extract_data(json_file_path, columns, multilinearity, s3path):
    # Read in raw json data.
    raw_df = spark.read.json(path=json_file_path, multiLine=multilinearity)

    # Write raw_data to storage.
    raw_df.write.format("json").save(f"{s3path}/raw_data.json", mode = "overwrite")

    # Expand "data" nested list into individual rows.
    exploded_df = raw_df.select(explode(raw_df.data))

    # Expand "data" array in each row into columns with associated headers using python list comprehension.
    data_w_columns = exploded_df.select(
        *(exploded_df["col"][i].alias(elem) for i, elem in enumerate(columns))
    )

    return data_w_columns
    """
  This function extracts data from a given json_file_path and reads it to a dataframe object.

  Args:
    json_file_path: 
      A string representing the path within the dbfs where your json file lives.
    multilinearity: 
      A boolean representing whether the file is single line json or multilinear.
    columns:
      A list of column headers for the extracted data.
    s3path:
      A string representing the path to store the read file for future access.

  Returns:
    A Pyspark DataFrame.
  """

# COMMAND ----------

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 1 here
from pyspark.sql.functions import explode
json_file_path = "dbfs:/tmp/user_12df1ddd/rows.json"
storage_file_path = "s3a://e2-interview-user-data/home/AROAUQVMTFU2DCVUR57M2:utamhank1@gmail.com"
columns = [
    "sid",
    "id",
    "position",
    "created_at",
    "created_meta",
    "updated_at",
    "updated_meta",
    "meta",
    "year",
    "first_name",
    "county",
    "sex",
    "count",
]

# Read in, and extract specific columns to top level from raw data with helper function.
data_w_columns = extract_data(
    json_file_path=json_file_path, columns=columns, multilinearity=True, s3path=storage_file_path  
)

# Create temp table from DataFrame.
data_w_columns.createOrReplaceTempView("baby_names")

# COMMAND ----------

# Sanity Tests for Question 1 (Would implement as unittests if given databricks repo permissions).
from pyspark.sql.functions import size, col

num_test_passed = 0

raw_df = spark.read.format("json").load(path="s3a://e2-interview-user-data/home/AROAUQVMTFU2DCVUR57M2:utamhank1@gmail.com/raw_data.json")

# Is the raw json DataFrame empty?
if len(raw_df.head(1)) > 0:
    num_test_passed += 1
    print("Raw json DataFrame is not empty................TEST PASSED.")

else:
    print("Raw json Dataframe is empty................TEST FAILED!")

# Get count of records in raw json DataFrame.
num_records_raw_json = (
    raw_df.withColumn("data_len", size(col("data"))).select("data_len").collect()[0][0]
)

# Get count of rows in output Dataframe.
rows_dataframe = data_w_columns.count()
print(f"The number of records in the raw json is: {num_records_raw_json}")
print(f"The number of rows in the output DataFrame is: {rows_dataframe}")

# Do the number of rows in the output dataframe match the number of elements in the raw json DataFrame?
if num_records_raw_json == rows_dataframe:
    num_test_passed += 1
    print(
        "Number of records in raw json matches number of rows in output DataFrame................TEST PASSED."
    )
else:
    print(
        "Number of elements in raw json do not match rows in DataFrame................TEST FAILED!"
    )

# Are there duplicates in the output DataFrame?
if data_w_columns.count() == data_w_columns.dropDuplicates(columns).count():
    num_test_passed += 1
    print("Output DataFrame has no duplicates................TEST PASSED.")
elif data_w_columns.count() > data_w_columns.dropDuplicates(columns).count():
    print("Ouput DataFrame has duplicate records................TEST FAILED!")
else:
    print("Output DataFrame is missing records................TEST FAILED!")

# Does the created view match the original number of records?
row_count_view = spark.sql("select count(*) as rowcount from baby_names").collect()[0][
    0
]

if num_records_raw_json == row_count_view:
    num_test_passed += 1
    print(
        "Rows in outputted view matches number of original records................TEST PASSED."
    )
elif num_records_raw_json >= row_count_view:
    print("Outputted view is missing records................TEST FAILED!")
else:
    print("Outputted view contains duplicate records................TEST FAILED!")

print(f"{num_test_passed}/4 TESTS PASSED SUCCESSFULLY.")

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.
# MAGIC ##### This code mainly uses a helper function *extract_data()*. 
# MAGIC This function (1) reads in the multi-line json file, (2) uses the spark sql function explode() to expand the nested array data structure defined by the "data" column name into multiple rows with one array each, and (3) uses python list comprehension to iterate over every element in the array in every row, alias the columns with the desired field names and outputs the dataframe with the desired fields extracted to the top-level. This function has O(N^2) time and space complexity due to the need to access every elem in every row of the exploded data structure in step(2), but is running as a pythonic list comprehension instead of a traditional for loop and therefore is more optimized in this context. 
# MAGIC
# MAGIC *One Caveat*: If the number of extracted columns will always be static as is assumed, then the algorithm would scale as **O(N)** in practice since the number of records per row would always be the same.
# MAGIC
# MAGIC After the dataframe has been created, we create a temp table using the createOrReplaceTempView() function.
# MAGIC
# MAGIC We then run a series of tests to validate the data and the associated created view.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 2 - Multiple Languages [10 Points]
# MAGIC
# MAGIC Using the temp table you created in the question above, write a SQL query that gives the most popular baby name for each year in the dataset. Then, write the same query using either the Scala or Python dataframe APIs.

# COMMAND ----------

# DBTITLE 1,Code Answer
# MAGIC %sql
# MAGIC /* Please provide your code answer for Question 2 here. You will need separate cells for your SQL answer and your Python or Scala answer.*/
# MAGIC /* Outer query to select the top baby name in each year based on total count of each name in the subquery */
# MAGIC SELECT
# MAGIC   YEAR,
# MAGIC   FIRST(FIRST_NAME) AS FIRST_NAME,
# MAGIC   MAX(TOTAL) OCCURRANCES
# MAGIC FROM
# MAGIC   /* Subquery to extract total count of each baby name in each year */
# MAGIC   (
# MAGIC     SELECT
# MAGIC       YEAR,
# MAGIC       FIRST_NAME,
# MAGIC       SUM(COUNT) AS TOTAL
# MAGIC     FROM
# MAGIC       BABY_NAMES
# MAGIC     GROUP BY
# MAGIC       FIRST_NAME,
# MAGIC       YEAR
# MAGIC     ORDER BY
# MAGIC       TOTAL DESC
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   YEAR
# MAGIC ORDER BY
# MAGIC   YEAR ASC

# COMMAND ----------

import time

# Time counter for runtime printing purposes.
startTimestamp = time.process_time()

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import year, first, max, sum
from pyspark.sql.window import Window


importTimestamp = time.process_time()
importTime = importTimestamp - startTimestamp
print(f"Package import runtime: {round(importTime*1000)} ms")

# Convert "count" column datatype from string to integer for aggregation.
data_w_columns_int_count = data_w_columns.withColumn(
    "COUNT", data_w_columns["COUNT"].cast(IntegerType())
)

castTimestamp = time.process_time()
castTime = castTimestamp - importTimestamp
print(f"Integer cast runtime: {round(castTime*1000)} ms")

# Calculate the total count of each baby name in each year (subquery in the SQL code).
total_counts_df = (
    data_w_columns_int_count.groupBy(year("YEAR").alias("YEAR"), "FIRST_NAME")
    .agg(sum("COUNT").alias("TOTAL"))
    .orderBy("TOTAL", ascending=False)
)

# Specify a window spec to calculate the name with the largest total count per year.
window_spec = Window.partitionBy("YEAR").orderBy(total_counts_df["TOTAL"].desc())
top_baby_names_ranked_disk = (
    total_counts_df.select(
        "Year", first("FIRST_NAME").over(window_spec).alias("FIRST_NAME"), "TOTAL"
    )
    .groupBy("YEAR")
    .agg(first("FIRST_NAME").alias("FIRST_NAME"), max("TOTAL").alias("OCCURRENCES"))
).write.save(f"{storage_file_path}/top_baby_names_ranked.parquet", mode="overwrite")

queryTimestamp = time.process_time()
queryTime = queryTimestamp - castTimestamp
print(f"Query runtime: {round(queryTime*1000)} ms")

top_baby_names_ranked = spark.read.load(f"{storage_file_path}/top_baby_names_ranked.parquet").orderBy("YEAR")
top_baby_names_ranked.show()

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.
# MAGIC #### SQL Code.
# MAGIC In this SQL query, we first run a subquery to calculate the occurences of each individual name in every year with a simple summation of the count column grouping by the name and the year and ordering by the summation. The outer query then selects only the first (and therefore the name with the highest count summation, since the subquery is ordered) first_name from the subquery for each year to output a table with the most popular baby name per year. This query runs with a space and time complexity of O(N) where N is the number of rows in the queried table.
# MAGIC
# MAGIC #### Python Code.
# MAGIC In the Python code we take a similar subquery approach using the DataFrame API. But before we do that, we have to prepare the data in the dataframe for mathematical operations, specifically, the *count* column, since it is by default a string-type. In lines 5-8 we cast the count column to an integer-type with the .cast() method.After the column is prepared, we first (1) replicate the subquery in the SQL code with the DataFrame API (lines 10-15). For the outer query (lines 17-27), we first implement window partitioning on the dataframe in step (1) to partition by year with the count of the name occurences, then select the first first_name from each partition on the highest count summation (max(TOTAL)) (representing the name the was the most popular in that given year). This code runs with a space and time complexity of O(N) where N is the number of rows in the DataFrame.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 3 - Performance [10 Points]
# MAGIC
# MAGIC Are there any performance considerations when choosing a language API (SQL vs Python vs Scala) in the context of Spark?
# MAGIC
# MAGIC Are there any performance considerations when using different data representations (RDD, Dataframe) in Spark? Please explain, and provide references if possible. No code answer is required.

# COMMAND ----------

# MAGIC %scala
# MAGIC /* Writing query code in scala for purposes of answering question #3: Do not grade for purposes of answering question #2 (Not written in best style as I am still learning). */
# MAGIC /* Create Spark DataFrame in scala. */
# MAGIC val data_w_columns_scala = spark.sql("select * from baby_names")

# COMMAND ----------

# MAGIC %scala
# MAGIC /* Writing query code in scala for purposes of answering question #3: Do not grade for purposes of answering question #2 (Not written in best style as I am still learning). */
# MAGIC
# MAGIC /* Time counter for runtime printing purposes */
# MAGIC val startTimestamp = System.currentTimeMillis()
# MAGIC
# MAGIC import org.apache.spark.sql.types.IntegerType
# MAGIC import org.apache.spark.sql.functions.{year, first, sum, max, desc}
# MAGIC import org.apache.spark.sql.functions.col
# MAGIC import org.apache.spark.sql.expressions.Window
# MAGIC
# MAGIC val importTimestamp = System.currentTimeMillis()
# MAGIC val importRuntime = importTimestamp - startTimestamp
# MAGIC println(s"Package import runtime: $importRuntime ms")
# MAGIC
# MAGIC /* Cast count column in DataFrame to integer-type. */
# MAGIC val data_w_columns_int_count_scala =
# MAGIC   data_w_columns_scala.withColumn("count", col("count").cast(IntegerType))
# MAGIC
# MAGIC val castTimestamp = System.currentTimeMillis()
# MAGIC val castRuntime = castTimestamp - importTimestamp
# MAGIC println(s"Integer cast runtime: $castRuntime ms")
# MAGIC
# MAGIC /* Calculate total count for each baby name per year (SQL code inner query). */
# MAGIC val total_count_df_scala = data_w_columns_int_count_scala
# MAGIC   .groupBy(year($"YEAR").alias("YEAR"), $"FIRST_NAME")
# MAGIC   .agg(sum($"COUNT").alias("TOTAL"))
# MAGIC   .orderBy(desc("TOTAL"))
# MAGIC
# MAGIC /* Create window specification for windowing function. */
# MAGIC val window = Window.partitionBy("YEAR").orderBy(desc("TOTAL"))
# MAGIC
# MAGIC /* Outer query using window function to calculate the most popular names per year. */
# MAGIC val top_baby_names_ranked_scala = total_count_df_scala
# MAGIC   .select(
# MAGIC     $"YEAR",
# MAGIC     first($"FIRST_NAME").over(window).alias("FIRST_NAME"),
# MAGIC     $"TOTAL"
# MAGIC   )
# MAGIC   .groupBy("YEAR")
# MAGIC   .agg(
# MAGIC     first($"FIRST_NAME").alias("FIRST_NAME"),
# MAGIC     max($"TOTAL").alias("OCCURENCES")
# MAGIC   )
# MAGIC   .orderBy("YEAR")
# MAGIC
# MAGIC val queryTimestamp = System.currentTimeMillis()
# MAGIC val queryRuntime = queryTimestamp - castTimestamp
# MAGIC println(s"Query runtime: $queryRuntime ms")
# MAGIC
# MAGIC top_baby_names_ranked_scala.show()
# MAGIC

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please write your written answer here.
# MAGIC #### *Are there any performance considerations when choosing a language API (SQL vs Python vs Scala) in the context of Spark?*
# MAGIC There are advantages and disadvantages to either of the 3 approaches to querying the data in the context of Apache Spark.
# MAGIC
# MAGIC ### SQL.
# MAGIC #### Advantages:
# MAGIC SQL is the most well-known and widely used querying language in the world, and the simplest to implement and understand by most technical and non-technical parties. Spark also offers a variety of SQL performance tuning functions such as caching and hints that can be used to reduce the time and space complexity of the query (https://spark.apache.org/docs/latest/sql-performance-tuning.html). SQL performs best on smaller, simpler query workloads against well-organized and indexed relational database tables. In the case of this assignment, this is why the SQL query seemed to perform the fastest.
# MAGIC #### Disadvantages: 
# MAGIC SQL is not a robust language for more complex analytical and calculation-oriented workloads (such as those required by more advanced data science or ML algorithms.). SQL also does not easily support programmatic workflow tools such as variables and unit testing.
# MAGIC
# MAGIC ### Python.
# MAGIC #### Advantages:
# MAGIC Python is the most popular and widely used general-purpose programming language in data engineering/data science. In the context of spark, the pyspark library brings the tools used in spark into any python VM. This, buttressed by the wide range of open source third party libraries, make python a great choice for complex analytical workflows and algorithms (such as those used in ML). 
# MAGIC #### Disadvantages:
# MAGIC Python is a *interpreted* as opposed to compiled programming language, which makes it slower for a large number of operations than a compiled language like Java. In addition, code written with PySpark needs to be translated for the Java Virtual Machine (JVM) used by Spark, this can result in some performance overhead. For larger computational tasks however, the performance benefits can outweigh the costs, but in the case of this assignment, the SQL code performed faster due to the nature of the queried data and the operations performed. Note however, that python code used to extract and clean the data from the raw json before it was in a state to be queried by SQL, so overall python is the most versatile of the two.
# MAGIC
# MAGIC ### Scala.
# MAGIC #### Advantages: 
# MAGIC Scala is a static, *compiled* general purpose programming language based on java, which is spark's original language. In theory, this leads to better performance with the underlying spark JVM engine. Libraries that are part of java can be natively run in scala and therefore the JVM. The compiled as opposed to interpretted nature of the language makes it faster for many operations. Scala has better support for functional programming techniques, which are generally faster than procedural ones due to avoidance of storing states and mutable data structures.
# MAGIC #### Disadvantages:
# MAGIC Scala has a more verbose syntax and a steeper learning curve which can make it tougher to work with for beginner programmers. On a single node cluster, scala would have a performance advantage (all else being equal), but due to spark's computationally distributed nature, and the performance advantages of scala over python seem to decrease the more compute clusters that there are. In the case of our data, it seems that the query performance of both python and scala were similar, with python even having a small edge when it came to query time. There could be a few reasons for this: (1) Our compute cluster has four nodes as opposed to 1, (2) Using the pyspark module's Py4j tool (which allows the API to interface with the JVM), there are certain optimizations that are being made "under the hood" which aren't possible with scala, since scala code is run directly against the JVM. One of these is Catalyst optimizer, which can optimize high level code into a more efficient execution plan than native scala can alone. These may explain the slightly better performance we are getting using Python.
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### *Are there any performance considerations when using different data representations (RDD, Dataframe) in Spark? Please explain, and provide references if possible. No code answer is required.*
# MAGIC
# MAGIC #### 
# MAGIC RDD stands for "Resilient Distributed Dataset" and is the original Spark client-facing api for working with elements of data. A DataFrame is built on top of an RDD and is organized into columns. A Dataset extends DataFrames by providing type-safety and an object-oriented interface.
# MAGIC
# MAGIC From a performance standpoint RDD's are the slowest, but offer the most low-level control over the data for more complex transformations and to access specific RDD operations for available in Dataframes or Datasets. RDD's are better for handling unstructured data but require one to manually define a schema and require serialization to encode the data so are thus more computationally expensive to maintain.
# MAGIC
# MAGIC DataFrames are generally the fastest for data operations due to their ability to leverage query optimizations through the catalyst optimizer, no need for Java serialization, and associated 'garbage collection', auto-detected schema, and ability to better take advantage of distributed computing. Datasets are similar but offer additional safeguards such as complile time type-safety and thus are only available on R and Scala (since they are compiled languages). Datasets are generally faster than RDD's but slgihtly slower than DataFrames for most data operations.
# MAGIC
# MAGIC ##### Sources (for question on RDD vs. DataFrame): 
# MAGIC ##### 
# MAGIC 1. https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
# MAGIC 2. https://phoenixnap.com/kb/rdd-vs-dataframe-vs-dataset
# MAGIC 3. https://www.analyticsvidhya.com/blog/2020/11/what-is-the-difference-between-rdds-dataframes-and-datasets/
# MAGIC 4. https://sparkbyexamples.com/spark/spark-rdd-vs-dataframe-vs-dataset/
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 4 - Nested XML [15 Points]
# MAGIC Imagine that a new upstream system now automatically adds an XML field to the JSON baby dataset.  The added field is called visitors. It contains an XML string with visitor information for a given birth. We have simulated this upstream system by creating another JSON file with the additional field.  
# MAGIC
# MAGIC Using the JSON dataset at dbfs:/interview-datasets/sa/births/births-with-visitor-data.json, do the following:
# MAGIC 1. Read the births-with-visitor-data.json file into a dataframe and parse the nested XML fields into columns and print the total record count.
# MAGIC 2. Find the county with the highest average number of visitors across all births in that county
# MAGIC 3. Find the average visitor age for a birth in the county of KINGS
# MAGIC 4. Find the most common birth visitor age in the county of KINGS
# MAGIC

# COMMAND ----------

visitors_path = "/interview-datasets/sa/births/births-with-visitor-data.json"

# COMMAND ----------

# DBTITLE 1,#1 - Code Answer
## Hint: the code below will read in the downloaded JSON files. However, the xml column needs to be given structure. Consider using a UDF.
import xml.etree.ElementTree as ET
from pyspark.sql.functions import explode

visitor_xml_schema = "array<struct<id:string, age:string, sex:string>>"


def xml_parser(key):
    root_node = ET.fromstring(key)
    return list(map(lambda t: t.attrib, root_node.findall("visitor")))


extract_xml_udf = udf(xml_parser, visitor_xml_schema)

df = spark.read.option("inferSchema", True).json(visitors_path)
df_with_parsed_xml_cols = df.select(
    "sid",
    "county",
    "created_at",
    "first_name",
    col("id").alias("birth_id"),  # Alias original id column to prevent ambiguity with parsed visitor id column.
    "meta",
    "name_count",
    "position",
    col("sex").alias("sex_assigned_birth"),  # Alias original sex column to prevent ambiguity with parsed visitor sex column.
    "updated_at",
    "year",
    explode(extract_xml_udf("visitors")).alias("visitors"),
).select("*", "visitors.*")

# Calculate total number of records.
num_rows = df_with_parsed_xml_cols.count()
df_with_parsed_xml_cols.show(10)
print(f"Total Record Count in XML parsed DataFrame: {num_rows}")

# Total number of records in original data structure:
num_records_raw_json_xml = df.count()

print(f"num_records_raw_json_xml = {num_records_raw_json_xml}")

# COMMAND ----------

# DBTITLE 1,Data case normalization.
from pyspark.sql.functions import upper, col, sum, count

# Capitalize all records to mitigate discrepancies.
df_with_parsed_xml_cols_caps = df_with_parsed_xml_cols.select(
    *[
        upper(col(x)).alias(x)
        for x in df_with_parsed_xml_cols.columns
        if x not in {"visitors"}
    ]
)

# Create temp view for querying.
df_with_parsed_xml_cols_caps.createOrReplaceTempView("baby_names_w_visitors")

# COMMAND ----------

# DBTITLE 1,Data integrity check (1): Duplicates.
# MAGIC %sql
# MAGIC /* Data integrity check with primary key exercise to figure out combination of columns to act as keys of data AND check if there are any duplicates in the outputted view. If the value of ID_CNT > 1 for any column, there are potential duplicates. */
# MAGIC SELECT
# MAGIC   COUNT(*) AS ID_CNT,
# MAGIC   COUNTY,
# MAGIC   ID AS VISITOR_ID,
# MAGIC   BIRTH_ID,
# MAGIC   SEX AS VISITOR_SEX,
# MAGIC   AGE AS VISITOR_AGE
# MAGIC FROM
# MAGIC   BABY_NAMES_W_VISITORS
# MAGIC GROUP BY
# MAGIC   COUNTY,
# MAGIC   VISITOR_ID,
# MAGIC   BIRTH_ID,
# MAGIC   VISITOR_SEX,
# MAGIC   VISITOR_AGE
# MAGIC ORDER BY
# MAGIC   ID_CNT DESC
# MAGIC LIMIT
# MAGIC   5
# MAGIC
# MAGIC /* Primary key exercise data investigation query (no longer needed once PK has been identified) */
# MAGIC --SELECT * FROM BABY_NAMES_W_VISITORS WHERE COUNTY = "WESTCHESTER" AND ID = "8357" AND BIRTH_ID = "00000000-0000-0000-2332-59BABEFD502D" AND SEX = "F"

# COMMAND ----------

# DBTITLE 1,Data integrity check (2): Total records.
# MAGIC %sql
# MAGIC /* Data integrity check to count the number of rows in the created view and ensure that it matches the dataframe with parsed xml -> Should be 176470. */
# MAGIC
# MAGIC SELECT COUNT(*) AS TOTAL_RECORDS FROM BABY_NAMES_W_VISITORS

# COMMAND ----------

# DBTITLE 1,Data integrity check (3): BIRTH_ID record count.
# MAGIC %sql
# MAGIC /* Data integrity check to ensure number unique BIRTH_ID's match the amount of records in the original json (with unparsed xml) -> should be 70499.*/
# MAGIC SELECT
# MAGIC   COUNT(DISTINCT(BIRTH_ID))
# MAGIC FROM
# MAGIC   BABY_NAMES_W_VISITORS

# COMMAND ----------

# DBTITLE 1,Data integrity check (4): Total visitors.
# MAGIC %sql
# MAGIC /* Data integrity check to count the number of total visitors which should equal the difference between the dataframe (with parsed xml) -> should be 176470.*/
# MAGIC
# MAGIC SELECT SUM(COUNT_VISITORS) AS TOTAL_VISITORS_IN_DATA FROM (
# MAGIC SELECT BIRTH_ID, COUNT(ID) AS COUNT_VISITORS FROM BABY_NAMES_W_VISITORS GROUP BY BIRTH_ID)

# COMMAND ----------

# DBTITLE 1,#2 - Code Answer
# Calculate total number of visitors per birth per county.
num_visitors_per_birth_per_county = df_with_parsed_xml_cols_caps.groupBy(
    "BIRTH_ID", "COUNTY"
).agg(count("ID").alias("NUM_VISITORS"))

# Calculate the county with the highest avg number of visitors per birth.
highest_avg_births_county_df = (
    num_visitors_per_birth_per_county.groupBy("COUNTY")
    .agg((sum("NUM_VISITORS") / count("BIRTH_ID")).alias("AVG_NUM_VISITORS"))
    .orderBy(col("AVG_NUM_VISITORS").desc())
)

county_w_highest_avg_visitors, avg_num_visits = highest_avg_births_county_df.limit(
    1
).collect()[0]

print(
    f"The county with the highest number of visitors per birth was {county_w_highest_avg_visitors} county with an avg number of visitors per birth of {round(avg_num_visits, 3)}."
)

# COMMAND ----------

# DBTITLE 1,#3 - Code Answer
# MAGIC %sql
# MAGIC /* Find the average visitor age for a birth in the county of KINGS */
# MAGIC SELECT
# MAGIC   ROUND(AVG(AGE)) AS AVERAGE_VISITOR_AGE_KINGS
# MAGIC FROM
# MAGIC   BABY_NAMES_W_VISITORS
# MAGIC WHERE
# MAGIC   COUNTY = "KINGS"

# COMMAND ----------

# DBTITLE 1,#4 - Code Answer
# MAGIC %sql
# MAGIC /* Find the most common birth visitor age in the county of KINGS */
# MAGIC SELECT
# MAGIC   AGE AS MOST_COMMON_VISITOR_AGE
# MAGIC FROM
# MAGIC   BABY_NAMES_W_VISITORS
# MAGIC WHERE
# MAGIC   COUNTY = "KINGS"
# MAGIC GROUP BY
# MAGIC   AGE
# MAGIC ORDER BY
# MAGIC   COUNT(*) DESC
# MAGIC LIMIT(1)
