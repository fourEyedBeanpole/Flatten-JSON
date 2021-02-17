from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pathlib import Path
import collections

# COMMAND ----------
sc = SparkContext(master="local", appName="Pyspark Reading multiple json")

spark = SparkSession(sparkContext=sc)

path = Path("C:\\Users\\sharm\\PycharmProjects\\pythonProject")

file_list = [file.name for file in path.iterdir() if file.suffix == '.json'] #creates a list of all the json files

# print(file_list)
final_schema = [] #final schema of all the json files

final_df = None #final dataframe which we can use anywhere


#checks whether the schema of incoming data frame is same
def check_schema(df1,df2):
    if collections.Counter(df1.columns) == collections.Counter(df2.columns):
        return True
    else:
        return False


#adds data and updates schema when the schema of the incoming dataframe is not same
def add_data(f_df, incoming_df):
    l1 = f_df.columns
    l2 = incoming_df.columns
    different_columns = [x for x in l1 + l2 if x not in l1 or x not in l2]
    if len(f_df.columns) > len(incoming_df.columns):
        for i in different_columns:
            # s1 = str(i)
            incoming_df = incoming_df.withColumn(i, lit(None))
    else:
        for i in different_columns:
            # s2 = str(i)
            final_schema.append(i)
            f_df = f_df.withColumn(i, lit(None))
    return f_df.union(incoming_df)

for f in file_list:
    paths = str(path) + "/"+ f
    multiline_df = spark.read.option("multiline", "true").json(paths)
    if len(final_schema) == 0:
        final_schema = multiline_df.columns
        final_df = multiline_df
    else:
        if check_schema(final_df, multiline_df):
            final_df = final_df.union(multiline_df)
        else:
            final_df = add_data(final_df,multiline_df)

final_df.show()





