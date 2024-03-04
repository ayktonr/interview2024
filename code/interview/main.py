#importing the libraries needed
from pyspark.sql import SparkSession

#creating a spark session
spark = SparkSession.builder.appName("Client data").getOrCreate()

#variables needed for the application
path_df1 = 'C:\\Users\\C61506\\interview\\input\\dataset_one.csv'
path_df2 = 'C:\\Users\\C61506\\interview\\input\\dataset_two.csv'
filter_var = 'Netherlands'
path_output = 'C:\\Users\\C61506\\interview\\client_data\\client_data.csv'


######################Functions############################

#this function is used to filter out a certain value from a data frame column
def filter_column(df, column):
    df= df.filter(df[column]==filter_var)
    return df

#this function is used to rename the columns needed
def rename_columns(df, column_dict):    
    for key, value in column_dict.items():
        df = df.withColumnRenamed(key, value)
    return df

#this function is used to drop the columns needed
def drop_columns(df, dropped_list):
    for value in dropped_list:
        df = df.drop(value)
    return df

#############################################################

#dataframes for the .csv files
df1 = spark.read.csv(path_df1, header=True)
df2 = spark.read.csv(path_df2, header=True)

#showing the original columns
print(df1.columns)
print(df2.columns)

#creating dictionaries that are needed for the functions
column_dict_df1 = {"id":"client_identifier"}
column_dict_df2 = {"id":"client_identifier", "btc_a": "bitcoin_address", "cc_t": "credit_card_type"}
dropped_list_df1 = {'first_name', 'last_name'}
dropped_list_df2 = {'cc_n'}

#calling functions to clean the dataframes
df1_filtered = filter_column(df1, 'country')
df1_dropped = drop_columns(df1_filtered, dropped_list_df1)
df1_final = rename_columns(df1_dropped, column_dict_df1)

df2_dropped = drop_columns(df2, dropped_list_df2)
df2_final = rename_columns(df2_dropped, column_dict_df2)


#joining 2 dataframes on the 'client_identifier'(aka 'id') column
joined_df = df1_final.join(df2_final, on='client_identifier', how='inner')
joined_df.show()

#writing the dataframe to a csv file under 'client_data' folder
joined_df.write.option("header", True).option("delimiter", ',').csv(path_output)

#stopping the SparkSession
spark.stop()


