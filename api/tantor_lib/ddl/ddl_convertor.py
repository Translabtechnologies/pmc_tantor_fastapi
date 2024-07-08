from tantor_lib.metadata_detail.db_ibmdb2 import Db2ConnectionManager
from tantor_lib.metadata_detail.db_mssql import MsSqlConectionManger
from tantor_lib.metadata_detail.db_mysql import MySqlConectionManger
from tantor_lib.metadata_detail.db_oracle import OracleConectionManger
from tantor_lib.metadata_detail.db_postgres import PostgresConectionManger
# from tantor.metadata_detail.db_oracle import OracleConectionManger
# from tantor.metadata_detail.db_mssql import MsSqlConectionManger
# from tantor.metadata_detail.db_mysql import MySqlConectionManger
# from tantor.metadata_detail.db_postgres import PostgresConectionManger
# from tantor.metadata_detail.db_ibmdb2 import Db2ConnectionManager
from .s_oracle import ddl_oracle_transformation
from .s_mssql import  ddl_mssql_transformation
from .s_postgres import ddl_postgres_transformation




def generate_query(source_metadata,table_name):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
        source_metadata (dict): Metadata or JSON representing the source table structure details.

    Returns:
        str: The SQL query string for creating the table.

    Note:
        The input `source_metadata` should contain information about the table's name, columns, data types,
        constraints, and other properties required for creating the table.
    """
    columns = source_metadata[0].get("column_json", [])
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            data_length = column.get('DATA_LENGTH', None)
            data_precision = column.get('DATA_PRECISION', None)
            data_scale = column.get('DATA_SCALE', None)
            column_type_str = data_type
            if data_type in ["VARCHAR", "NVARCHAR", "INT", "CHAR", "BINARY", "VARBINARY", "FLOAT"]:
                if data_length is not None:
                    column_type_str += f"({data_length})"
                else:
                    column_type_str += "(MAX)"
            elif data_type == "FLOAT":
                if data_precision is not None and data_scale is not None:
                    column_type_str += f"({data_precision}, {data_scale})"
                elif data_precision is not None:
                    column_type_str += f"({data_precision})"
            elif data_type == "NUMERIC":
                if data_precision is not None and data_scale is not None:
                    column_type_str += f"({data_precision}, {data_scale})"
                elif data_precision is not None:
                    column_type_str += f"({data_precision})"
            # else:
            #     column_type_str = column_type_str

            create_table_query += f"\t`{column_name}` {column_type_str},\n"       

        create_table_query = create_table_query.rstrip(",\n")  # Remove the trailing comma and newline
        create_table_query += "\n);"
        return create_table_query
    except Exception as e:
        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None


def check_table_on_target(target_conf, table_name):
    """
        this function use for creating the table on target.
    Parameters:
        target_conf: it contains the connection details. 
        table_name: name of table which we are tring to find out.
    returns:
        result: it return the table is created or not.  
    """
    result = True
    if target_conf['connection_type'] == 'oracle':
        oracle_conn = OracleConectionManger(target_conf)
        result = oracle_conn.find_table(table_name)
        oracle_conn.connection_close()
    elif target_conf['connection_type'] == 'mssql':
        mssql_conn = MsSqlConectionManger(target_conf)
        result = mssql_conn.find_table(table_name)
        mssql_conn.connection_close()
    elif target_conf['connection_type'] == 'mysql':
        mysql_conn = MySqlConectionManger(target_conf)
        result = mysql_conn.find_table(table_name)
        mysql_conn.connection_close()
    elif target_conf['connection_type'] == 'postgres':
        postgres_conn = PostgresConectionManger(target_conf)
        result = postgres_conn.find_table(table_name)
        postgres_conn.connection_close()
    elif target_conf['connection_type'] == 'db2':
        db2_conn = Db2ConnectionManager(target_conf)
        result = db2_conn.find_table(table_name)
        db2_conn.connection_close()
    
    print("result-------------------------------", result)
    return result


def execute_query_on_target(target_conf, sql_query):
    """
    this function use for creating the table on target.
    Parameters:
        target_conf: it contains the connection details. 
        sql_query: it contains the DDL query.
    returns:
        result: it return the table is created or not.  
    """
    result = True
    print("-----------Create Table-------", sql_query)
    if target_conf['connection_type'] == 'oracle':
        oracle_conn = OracleConectionManger(target_conf)
        result = oracle_conn.create_table(sql_query, table_create=True)
        oracle_conn.connection_close()

    elif target_conf['connection_type'] == 'mssql':
        mssql_conn = MsSqlConectionManger(target_conf)
        print('**************************************************')
        result = mssql_conn.create_table(sql_query,table_create=True)
        mssql_conn.connection_close()

    elif target_conf['connection_type'] == 'mysql':    #################19-10
        mysql_conn = MySqlConectionManger(target_conf)
        result = mysql_conn.create_table(sql_query, table_create=True)
        mysql_conn.connection_close()

    elif target_conf['connection_type'] == 'postgres':    #################20-10
        postgres_conn = PostgresConectionManger(target_conf)
        result = postgres_conn.create_table(sql_query, table_create=True)
        postgres_conn.connection_close()
    
    if target_conf['connection_type'] == 'db2':
        db2_conn = Db2ConnectionManager(target_conf)
        result = db2_conn.create_table(sql_query, table_create=True)
        db2_conn.connection_close()

    return result


def execute_constraint_query_on_target(target_conf, list_of_query):
    """
    this function use for creating the table on target.
    Parameters:
        target_conf: it contains the connection details. 
        sql_query: it contains the DDL query.
    returns:
        result: it return the table is created or not.  
    """
    result = True
    print('List_of_query---------------->', list_of_query)
    if target_conf['connection_type'] == 'oracle':
        oracle_conn = OracleConectionManger(target_conf)
        for each_query in list_of_query:
            try:
                result = oracle_conn.create_table(each_query, table_create=False)
            except Exception as err:
                print(f"Error! {err}")
                print(f"Error! for  query {each_query}")
        oracle_conn.connection_close()

    elif target_conf['connection_type'] == 'mssql':
        mssql_conn = MsSqlConectionManger(target_conf)
        for each_query in list_of_query:
            try:
                result = mssql_conn.create_table(each_query,table_create=False)
            except Exception as err:
                print("Error! for  query ")
        mssql_conn.connection_close()

    elif target_conf['connection_type'] == 'mysql':    #################19-10
        mysql_conn = MySqlConectionManger(target_conf)
        for each_query in list_of_query:
            try:
                result = mysql_conn.create_table(each_query, table_create=False)
            except Exception as err:
                print("Error! for  query ")
        mysql_conn.connection_close()

    elif target_conf['connection_type'] == 'postgres':    #################20-10
        postgres_conn = PostgresConectionManger(target_conf)
        for each_query in list_of_query:
            try:
                result = postgres_conn.create_table(each_query, table_create=False)
            except Exception as err:
                print("Error! for  query ")
        postgres_conn.connection_close()
    
    if target_conf['connection_type'] == 'db2':
        db2_conn = Db2ConnectionManager(target_conf)
        for each_query in list_of_query:
            try:
                result = db2_conn.create_table(each_query, table_create=False)
            except Exception as err:
                    print("Error! for  query ")
        db2_conn.connection_close()

    return result