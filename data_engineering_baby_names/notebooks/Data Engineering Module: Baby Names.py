# Databricks notebook source
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
  raw_df = spark.read.json(path = json_file_path, multiLine = multilinearity)

  # Expand "data" nested list into individual rows.
  exploded_df = raw_df.select(explode(raw_df.data))
  
  # Expand "data" array in each row into columns with associated headers using python list comprehension.
  data_w_columns = exploded_df.select(*(exploded_df["col"][i].alias(elem) for i, elem in enumerate(columns)))

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
json_file_path = "dbfs:/tmp/user_12df1ddd/rows.json"
columns = ["sid", "id", "position", "created_at", "created_meta", "updated_at", "updated_meta", "meta", "year", "first_name", "county", "sex", "count"]

# Read in, and extract specific columns to top level from raw data with helper function.
data_w_columns = extract_data(json_file_path = json_file_path, columns = columns, multilinearity = True)

# Create temp table from DataFrame.
data_w_columns.createOrReplaceTempView("baby_names")

# COMMAND ----------

# DBTITLE 1,Written Answer
# MAGIC %md
# MAGIC Please provide your brief, written description of your code here.

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
