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