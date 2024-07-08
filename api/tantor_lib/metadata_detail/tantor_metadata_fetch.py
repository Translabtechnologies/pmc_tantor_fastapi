import requests
import json
from datetime import datetime
from tantor_lib.metadata_detail.db_ibmdb2 import Db2ConnectionManager
from tantor_lib.metadata_detail.db_mssql import MsSqlConectionManger
from tantor_lib.metadata_detail.db_mysql import MySqlConectionManger
from tantor_lib.metadata_detail.db_oracle import OracleConectionManger
from tantor_lib.metadata_detail.db_postgres import PostgresConectionManger
from tantor_lib.metadata_detail.db_hive import HiveConnectionManager
from tantor_lib.metadata_detail.db_snowflake import SnowflakeConnectionManager
from tantor_lib.enp_dcp import MY_KEY, decrypt

headers={"Content-Type": "application/json"}

def generate_metadata_details(connection_info, table_name=None):
    try:
        results = []
        if connection_info['connection_type'] == 'oracle':
            oracle_connection = OracleConectionManger(connection_info)
            temp_results = oracle_connection.metadata_details(table_name)
            oracle_connection.connection_close()

        elif connection_info['connection_type'] == 'hive':
            hive_connection = HiveConnectionManager(connection_info)
            temp_results = hive_connection.metadata_details(table_name)

        elif connection_info['connection_type'] == 'snowflake':
            snowflake_connection = SnowflakeConnectionManager(connection_info)
            temp_results = snowflake_connection.metadata_details(table_name)

        for each in temp_results:
            if len(each['column_detail'])!=0: 
                temp = {"connection_id": connection_info.get('connection_id'),
                        "connection_type": connection_info.get('connection_type'),
                        "connection_name": connection_info.get('connection_name'),
                        "table_name": each.get('table_name'),
                        'table_schema': each.get('table_schema'),
                        "column_json": each.get('column_detail'),
                        "constraint_json": {} if len(each.get('constraint_details')) == 0 else each.get('constraint_details'),
                        "index_json": {} if len(each.get('index_details')) == 0 else each.get('index_details'),
                        "partition_json": {} if len(each.get('partition_json')) == 0 else each.get('partition_json'),
                        "is_migrated":True,
                        "is_purged": True,
                        'extracted_at':str(datetime.now()),
                        "created_by":connection_info.get('created_by'),
                        'project': connection_info.get('project')
                        }
                results.append(temp)
    except Exception as err:
        print("----temp error ------", err)
        return True, results
    return False, results

def compare_dict(d1,d2):
    import hashlib, json    
    hash1=hashlib.md5(json.dumps(d1, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()
    hash2=hashlib.md5(json.dumps(d2, sort_keys=True, ensure_ascii=True).encode('utf-8')).hexdigest()
    return hash1==hash2



def update_connection_metadata_table(results, endpoint_connection_metadata, table_name_metadata=None):
    list_table = []
    for post_data in results:
        table_name=post_data['table_name']           
        list_table.append(table_name)
        if table_name_metadata is not None:
            if table_name in list(table_name_metadata.keys()):  # update The existing
                metadata_info = table_name_metadata[table_name]
                temp1={
                    'column_json':metadata_info['column_json'],
                    'constraint_json':  metadata_info['constraint_json'],
                    'index_json':metadata_info['index_json'],
                    'partition_json':metadata_info['partition_json']
                }
                temp2={
                    'column_json':post_data['column_json'],
                    'constraint_json':  post_data['constraint_json'],
                    'index_json':post_data['index_json'],
                    'partition_json':post_data['partition_json']
                }
                if compare_dict(temp1,temp2):
                    print(f"No change found in Metadata for table '{table_name}'")
                else:
                    post_data["conn_metadata_id"] = metadata_info['conn_metadata_id']
                    url = f'{endpoint_connection_metadata}/{metadata_info["conn_metadata_id"]}'
                    resp = requests.patch(url, data=json.dumps(post_data), headers=headers, verify=False)
                    if resp.status_code == 204:
                        print(f"Succesfully update metadata for table '{post_data['table_name']}'")
                    else:
                        print(f"Failed to update metadata for table '{post_data['table_name']}'")
            else:  # insert the new one
                url = f'{endpoint_connection_metadata}'
                resp = requests.post(url, data=json.dumps(post_data), headers=headers, verify=False)
                if resp.status_code == 200:
                        print(f"Succesfully Insert metadata for table '{post_data['table_name']}'")
                else:
                        print(f"Failed to insert metadata for table '{post_data['table_name']}'")
        else:  # insert the new one
            url = f'{endpoint_connection_metadata}'
            resp = requests.post(url, data=json.dumps(post_data), headers=headers, verify=False)
            if resp.status_code == 200:
                print(f"Succesfully update metadata for table '{post_data['table_name']}'")
            else:
                print(f"Failed to update metadata for table '{post_data['table_name']}'")
    return list_table



def update_connection_status(connection_info, endpoint_connection):
    print('this us coonection_info--------->', connection_info)
    print(connection_info['connection_id'])
    if 'connection_id' in connection_info.keys():
        del connection_info['connection_id']
    resp = requests.put(f"{endpoint_connection}/{connection_info['connection_id']}", data=json.dumps(connection_info),headers=headers, verify=False)
    return resp


def get_metadata_table_list(endpoint_connection_metadata, conn_name):
    resp = requests.get(f"{endpoint_connection_metadata}={conn_name}",headers=headers, verify=False)
    if resp.status_code ==200:
        print(f"successfully get the metadata information for connection name '{conn_name}'")    
        return resp.json()
    else:
        print(f"failed to get the metadata information for connection name '{conn_name}'")
        return []



def delete_metadata_for_table(conn_name, table_name, endpoint_connection_metadata):
    print("--------delete_table------------", table_name)
    url = f'{endpoint_connection_metadata}/delete-by-conn-name/{conn_name}/"{table_name}"'
    print("delete------url", url)
    resp = requests.delete(url, headers=headers, verify=False)
    print("-------", resp.status_code)
    if resp.status_code == 204:
        print("Success to delete")
    else:
        print("Fail to delete")



def get_metadata_workflow_info(metadata_id, endpoint_metadata):
    if metadata_id:
        resp = requests.get(f"{endpoint_metadata}/{metadata_id}", headers=headers, verify=False)
        return resp.json()
    else:
        return None

def get_metadata_workflow_status_updated(metadata_id,data ,endpoint_metadata):
    print("-----------get_metadata_workflow_status_updated-----", data)
    resp = requests.patch(f"{endpoint_metadata}/{metadata_id}", data=json.dumps(data),headers=headers, verify=False)
    print(resp.status_code)
    print(resp.url)

    return resp


def get_enable_disable_dag(status, dag_id, endpoint_connection):
    resp = requests.patch(f"{endpoint_connection}{dag_id}",data=json.dumps({"is_paused": status}),headers=headers, verify=False)
    print(resp.url)
    return resp
    

def delete_table_for_connection(conn_info, table_name):
    """
    this function used to check the total space consume by table in MBS
    conn_info: you have to pass the connection details of connection
    table_name: to pass the name of table for which we are calculate the space
    return:
        table_space: return the size of table in Mbs 
    """
    result = True
    sql_query= f'DROP TABLE IF EXISTS {table_name}'
    print("-----------delete Table-------", sql_query)
    if conn_info['connection_type'] == 'oracle':
        oracle_conn = OracleConectionManger(conn_info)
        sql_query= f'DROP TABLE {table_name}'
        result = oracle_conn.delete_table(sql_query)
        oracle_conn.connection_close()

    elif conn_info['connection_type'] == 'mssql':
        mssql_conn = MsSqlConectionManger(conn_info)
        result = mssql_conn.delete_table(sql_query)
        mssql_conn.connection_close()

    elif conn_info['connection_type'] == 'mysql':    
        mysql_conn = MySqlConectionManger(conn_info)
        result = mysql_conn.delete_table(sql_query)
        mysql_conn.connection_close()

    elif conn_info['connection_type'] == 'postgres':    
        postgres_conn = PostgresConectionManger(conn_info)
        result = postgres_conn.delete_table(sql_query)
        postgres_conn.connection_close()
    
    if conn_info['connection_type'] == 'db2':
        db2_conn = Db2ConnectionManager(conn_info)
        result = db2_conn.create_table(sql_query)
        db2_conn.connection_close()

    return result
    
def test_connectivity(connection_info):
    if connection_info['connection_type'] == 'oracle':
        oracle_connection = OracleConectionManger(connection_info)
    
    elif connection_info['connection_type'] == 'hive':
        hive_connection = HiveConnectionManager(connection_info)

    elif connection_info['connection_type'] == 'snowflake':
        snowflake_connection = SnowflakeConnectionManager(connection_info)



def process_connection_info(connection_info,endpoint_metadata_by_conn_name,endpoint_connection_metadata,endpoint_connection):
    conn_name=connection_info.get('connection_name')
    all_table_name_metadata= get_metadata_table_list(endpoint_metadata_by_conn_name, conn_name)
    table_name_metadata={}
    for each in all_table_name_metadata:
        table_name_metadata[each.get('table_name')]=each
    print(f" for connection name {conn_name}, number of table's are {len(table_name_metadata.keys())}")
    print('Table_name_metadata', all_table_name_metadata)
    try:
        connection_info['password'  ]= decrypt(connection_info['password'], MY_KEY)
    except:
        pass
    err, results = generate_metadata_details(connection_info)
    if err:
        connection_info['status']='inactive'
    else:
        connection_info['status']='active'
        available_list_table=update_connection_metadata_table(results,endpoint_connection_metadata, table_name_metadata)
        list_of_deleted_table=[]
        if table_name_metadata :
            for each in list(table_name_metadata.keys()):
                if each not in available_list_table:
                    list_of_deleted_table.append(each)
        # print("list of deleted ",list_of_deleted_table)
        if len(list_of_deleted_table)>0:
            for each in list_of_deleted_table:
                delete_metadata_for_table(conn_name,each, endpoint_connection_metadata)
    # update_connection_status(connection_info, endpoint_connection)


