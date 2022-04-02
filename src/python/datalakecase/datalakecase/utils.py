import os, socket, sys, logging
import pyspark.sql.functions as F
import pyspark.sql.types as T
import json
import pyspark


def describe_dataframe(df):
    """Describe a given dataframe by printing key metrics.

    Metrics:
    - number_of_rows
    - number_of_unique_rows
    - number_of_duplicated_rows (%)
    
    """    
    number_of_rows = df.count()
    number_of_unique_rows = df.dropDuplicates().count()
    number_of_duplicated_rows = number_of_rows - number_of_unique_rows
    
    percent_of_unique_rows = number_of_unique_rows/number_of_rows * 100
    percent_of_duplicated_rows = number_of_duplicated_rows/number_of_rows * 100
    
    print("Total      rows: {}".format(number_of_rows))
    print("Unique     rows: {} ({:.2f}%)".format(number_of_unique_rows,percent_of_unique_rows))
    print("Duplicated rows: {} ({:.2f}%)".format(number_of_duplicated_rows, percent_of_duplicated_rows))

    
def add_prefix_to_df_columns(df, prefix="", separator="_", escape=[]):
    """
    Adds prefix to a DF ignoring current prefixes
    
    """
    
    for c in df.columns:
        if (c not in escape) and (prefix not in c) :
            df = df.withColumnRenamed(c, '{}{}{}'.format(prefix, separator, c))
    return df


def validate_json(record):
    """
    Fixing common problems found in JSON lint
    """
    
    try:
        updated_record = record.replace(r'\", "', '\ ", "')
        updated_record = updated_record.replace(r'\"}', '\ "}')      
        updated_record = updated_record.replace(r'\"', '\"')      
      
        json.loads(updated_record)   # trying to parse...
        return '{ "data" : '+ updated_record +' }'
        
    except Exception as e:
        
        return 'invalid'
    

def anonimize_columns(df, target_columns=[], prefix=None, numBits=256):

    """
    Applies a SHA-2 family of hash functions to columns (replace)
    """    
    
    for column in target_columns:
        if(prefix):
            df = df.withColumn(column, F.sha2(F.concat(prefix, "_", F.col(column).cast("string")), numBits))
        else:
            df = df.withColumn(column, F.sha2(F.col(column).cast("string"), numBits))
    return df

def compare_dfs(key_dfs=[]):

    """
    Compare key dfs distinct values in order to validade possible missing data
    """    
      
    counts = []
    
    for df in key_dfs:       
        counts.append(df.dropDuplicates().count())
    
    if not (all(x == counts[0] for x in counts)):
        raise Exception('Dataframes do not match count. Please check the pipe. Aborting. {}'.format(counts))
    
    return counts
