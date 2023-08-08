# -*- coding: utf-8 -*-
""" This module contains several modularized helper functions to assist in the task completion of the data engineering baby_names module.
"""

def extract_data(json_file_path, columns, multilinearity):

  # Read in raw json data.
  raw_df = spark.read.json(path = json_file_path, multiLine = multilinearity)
  # Expand "data" nested list into individual rows.
  exploded_df = raw_df.select(explode(raw_df.data))
  # Expand "data" array in each row into columns with associated headers using python list comprehension.
  data_w_columns = exploded_df.select(*(exploded_df["col"][i].alias(elem) for i, elem in enumerate(columns)))

  return raw_df
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