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


def extract_data(json_file_path, columns, multilinearity):
    # Read in raw json data.
    raw_df = spark.read.json(path=json_file_path, multiLine=multilinearity)

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

  Returns:
    A Pyspark DataFrame.
  """

# COMMAND ----------

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 1 here
from pyspark.sql.functions import explode
json_file_path = "dbfs:/tmp/user_12df1ddd/rows.json"
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
    json_file_path=json_file_path, columns=columns, multilinearity=True
)

# Create temp table from DataFrame.
data_w_columns.createOrReplaceTempView("baby_names")


# COMMAND ----------

# Sanity Tests for Question 1 (Would implement as unittests if given databricks repo permissions).
from pyspark.sql.functions import size, col

num_test_passed = 0
raw_df = spark.read.json(path=json_file_path, multiLine=True)

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
# MAGIC ###### This function (1) reads in the multi-line json file, (2) uses the spark sql function explode() to expand the nested array data structure defined by the "data" column name into multiple rows with one array each, and (3) uses python list comprehension to iterate over every element in the array in every row, alias the columns with the desired field names and outputs the dataframe with the desired fields extracted to the top-level. This function has O(N) time and space complexity, where N is the size of the JSON data. 
# MAGIC
# MAGIC ###### After the dataframe has been created, we create a temp table using the createOrReplaceTempView() function.
# MAGIC
# MAGIC ###### We then run a series of tests to validate the data and the associated created view.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 2 - Multiple Languages [10 Points]
# MAGIC
# MAGIC Using the temp table you created in the question above, write a SQL query that gives the most popular baby name for each year in the dataset. Then, write the same query using either the Scala or Python dataframe APIs.

# COMMAND ----------

# DBTITLE 1,Code Answer
# Please provide your code answer for Question 2 here. You will need separate cells for your SQL answer and your Python or Scala answer.

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 3 - Performance [10 Points]
# MAGIC
# MAGIC Are there any performance considerations when choosing a language API (SQL vs Python vs Scala) in the context of Spark?
# MAGIC
# MAGIC Are there any performance considerations when using different data representations (RDD, Dataframe) in Spark? Please explain, and provide references if possible. No code answer is required.

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please write your written answer here.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Baby Names Question 4 - Nested XML [15 Points]
# MAGIC Imagine that a new upstream system now automatically adds an XML field to the JSON baby dataset.  The added field is called visitors. It contains an XML string with visitor information for a given birth. We have simulated this upstream system by creating another JSON file with the additional field.  
# MAGIC
# MAGIC Using the JSON dataset at dbfs:/interview-datasets/sa/births/births-with-visitor-data.json, do the following:
# MAGIC 0. Read the births-with-visitor-data.json file into a dataframe and parse the nested XML fields into columns and print the total record count.
# MAGIC 0. Find the county with the highest average number of visitors across all births in that county
# MAGIC 0. Find the average visitor age for a birth in the county of KINGS
# MAGIC 0. Find the most common birth visitor age in the county of KINGS
# MAGIC

# COMMAND ----------

visitors_path = "/interview-datasets/sa/births/births-with-visitor-data.json"

# COMMAND ----------

# DBTITLE 1,#1 - Code Answer
## Hint: the code below will read in the downloaded JSON files. However, the xml column needs to be given structure. Consider using a UDF.
#df = spark.read.option("inferSchema", True).json(visitors_path)


# COMMAND ----------

# DBTITLE 1,#2 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#3 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#4 - Code Answer
## Hint: check for inconsistently capitalized field values. It will make your answer incorrect.

# COMMAND ----------

# DBTITLE 1,#4 - Written Answer
# MAGIC %md
# MAGIC Please provide your written answer for Question 4 here
