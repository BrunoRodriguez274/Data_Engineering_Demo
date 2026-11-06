import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import pandas as pd
import glob
#from pyspark.dbutils import DBUtils

spark = SparkSession.getActiveSession()

############################################################################
# General Functions 
############################################################################

def my_msg():
    print("Iniciando")
    return None

def binary_to_long(binary_data):
    """Converts Binary object to Long"""

    if binary_data is None:
        return None
    return int.from_bytes(binary_data, byteorder='big', signed=False)

#binary_to_long_udf = F.udf(binary_to_long, T.LongType())

def get_tables_list(catalog_, schema_, func_=lambda x: True):
    """ Create a list of all tables in schema"""

    schema_tables = spark.catalog.listTables(f'{catalog_}.{schema_}')
    tables_list = [i.name for i in schema_tables if i.tableType == "MANAGED"]
    tables_list = list(filter(lambda x: func_(x), tables_list))

    return tables_list

def get_views_list(catalog_, schema_, func_=lambda x: True):
    """ Create a list of all views in schema"""

    schema_tables = spark.catalog.listTables(f'{catalog_}.{schema_}')
    views_list = [i.name for i in schema_tables if i.tableType == "VIEW"]
    views_list = list(filter(lambda x: func_(x), views_list))

    return views_list

def pandas_to_spark_dataframe(table_path_):
    source_table = pd.read_csv(table_path_)
    return spark.createDataFrame(source_table, schema=list(source_table.columns))

def df_to_html(df_):

    header = "".join([f" <th> {col} </th> " for col in df_.columns])
    
    table_contents = f""
    for row in df_.select("*").collect():
        table_contents += f" <tr> "
        for value in row:
            table_contents += f" <td> {value} </td>"
        table_contents +=  f" </tr> "

    return f"<tr> {header} </tr>  {table_contents}"

def table_simple_upsert(UC_catalog_, UC_schema_, table_name_, df_, conditions_:list = [], partitions_ = []) -> None:
    
    table_full_path = f"{UC_catalog_}.{UC_schema_}.{table_name_}"

    if spark.catalog.tableExists(table_full_path) == False:
        print(f"Table {table_full_path} doesn't exist. Creating table...")
        
        df_ = df_.write.mode("overwrite")
        
        if len(partitions_) >0:
            df_ = df_.partitionBy(*partitions_)

        df_.saveAsTable(table_full_path)

        try:
            print(f"Inserted {df_.count()} records")
        except:
            print(f"Inserted {spark.table(table_full_path).count()}")
            
        return None

    if len(conditions_) == 0:
        raise AssertionError("No conditions given")
    
    merge_conditions = " AND ".join([f"IF((source_tbl.{condition} IS NULL AND target.{condition} IS NULL), TRUE, source_tbl.{condition} = target.{condition})" for condition in conditions_])
    #print(merge_conditions)
    target_table = DeltaTable.forName(spark, f"{UC_catalog_}.{UC_schema_}.{table_name_}")
    #target_table = spark.table()

    skip_same_condition = " OR ".join([f"IF((source_tbl.{col} IS NULL AND target.{col} IS NULL), FALSE, source_tbl.{col} != target.{col})" for col in df_.columns if col not in ["Updated_At"]])

    #print(skip_same_condition)
    merge_result = target_table.alias("target").merge(df_.alias("source_tbl"), merge_conditions)\
        .whenMatchedUpdateAll(condition = skip_same_condition)\
        .whenNotMatchedInsertAll()\
            .execute()

    metrics = merge_result
    display(metrics)
    if metrics is not None:
        print(metrics.show())
    return None

def read_from_sql_server(query_, host_, user_, password_, database_, port_):
    remote_table = (spark.read.format("sqlserver")  
    .option("port", port_)
    .option("host", host_)
    .option("user", user_)
    .option("password", password_)
    .option("database", database_)
    .option("trustServerCertificate", "true")
    .option("query", query_)
    .load()
    )
    return remote_table

def move_volume_file(source_path_: str, target_path_: str, file_name_: str, overwrite_: bool = False) -> str:
    """
    Move a file within Databricks Volumes.

    source_path: full path to the file, e.g. /Volumes/catalog/schema/volume/in/data.csv
    destination: a target folder (ending in '/') or a full target file path
    overwrite:   if False, raises an error when the target already exists
    """
    # Make sure the source exists
    try:
        dbutils.fs.ls(source_path_ + file_name_)
    except Exception:
        raise FileNotFoundError(f"Source file {file_name_} not found in: {source_path_}")

    # Check whether the target already exists
    try:
        dbutils.fs.ls(target_path_)
        if not overwrite_:
            raise FileExistsError(f"File already exists: {target_path_}")
        dbutils.fs.rm(target_path_)
    except FileExistsError:
        raise
    except Exception:
        pass  # target doesn't exist, which is fine

    dbutils.fs.mv(source_path_ + file_name_, target_path_ + file_name_)

    return target_path_


def read_excel(file_path_: str, schema_= None,  **kwargs):
    """
    Read excel from external location as a DataFrame. You can pass option arguments as a python dictionary.
    """
 
    df = spark.read.format("excel")
 
    for i in kwargs.keys():
        df = df.option(i, kwargs[i])
   
    if schema_ is not None:
        df = df.schema(schema_)
 
    df = df.load(file_path_)
    return df
 
def save_as_parquet(df_ , target_path_ : str, **kwargs):
    """
    Save dataframe as parquet on defined location
    """
 
    # return df_.repartition(1).write.mode("overwrite").parquet(target_path_, **kwargs)
    df_.coalesce(1).write.format("parquet").mode("overwrite").save(target_path_)
        # Find the Parquet file in the temporary folder
    files = dbutils.fs.ls(target_path_)
    output_file = next(x for x in files if x.name.startswith("part-"))
 
    temporal_path = target_path_.replace(".parquet", "_temp.parquet")
    dbutils.fs.mv(output_file.path, temporal_path)
 
    # Delete filepath, if it exists
    try:
        dbutils.fs.ls(target_path_)
        # Filepath exists
        print(f"Deleting old {target_path_}")
        dbutils.fs.rm(target_path_, recurse=True)
    except Exception as e:
        if "java.io.FileNotFoundException" not in str(e):
            raise e
 
    # Move the Parquet file to the final location
    dbutils.fs.mv(temporal_path, target_path_)
 
    # Delete temporary folder
    dbutils.fs.rm(temporal_path, True)
    print(f"Wrote '{target_path_}'")