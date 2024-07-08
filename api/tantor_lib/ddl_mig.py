from datetime import datetime
from tantor_lib.metadata_detail.tantor_metadata_fetch import generate_metadata_details, update_connection_metadata_table, update_connection_status
from tantor_lib.ddl.ddl_convertor import check_table_on_target, execute_query_on_target, execute_constraint_query_on_target
from tantor_lib.ddl.s_mssql import ddl_mssql_transformation
from tantor_lib.ddl.s_oracle import ddl_oracle_transformation,generate_oracle_alter_constarint_query
from tantor_lib.ddl.s_mysql import ddl_mysql_transformation
from tantor_lib.ddl.s_ibmdb2 import ddl_db2_transformation
from tantor_lib.ddl.s_postgres import ddl_postgres_transformation
from tantor_lib.enp_dcp import decrypt, MY_KEY
from tantor_lib.enpoint_list import endpoint_list
from tantor_lib.metadata_detail.tantor_metadata_fetch import get_metadata_workflow_status_updated


endpoint_connection_metadata = endpoint_list.get('endpoint_connection_metadata')
endpoint_connection = endpoint_list.get('endpoint_connection')
endpoint_metadata= endpoint_list.get('endpoint_metadata')

def ddl_dag(dag_info):
    # pass
    import ast
    data_json = ast.literal_eval(dag_info)
    source_info = data_json['source']
    target_info = data_json['target']
    try:   
        source_info['password'] = decrypt(source_info.get('password'), MY_KEY)
        target_info['password'] = decrypt(target_info.get('password'), MY_KEY)
        
    except Exception as e:
        print("Decryption failed:", e)

    new_table_name = data_json['table_name']
    print('----------new_table_name-------', new_table_name)
    result = check_table_on_target(target_info, new_table_name)
    if result:
        print('------result---------', result)
        return 'create_table_query'
    else:
        return 'alter_table'
    
    



def generate_alter_table_query(dag_info):
    import ast
    data_json = ast.literal_eval(dag_info)
    metaworkflow_id=data_json.get('id')
    get_metadata_workflow_status_updated(metaworkflow_id,{ "Status": "Table already exists"} ,endpoint_metadata)


def generate_create_table_query(dag_info):
    print('---------dag_info-----------', dag_info)
    import ast
    data_json = ast.literal_eval(dag_info)
    print('-----metadata-----', data_json)
    source_info = data_json['source']
    target_info = data_json['target']
    metadata = data_json
    table_name =  data_json['table_name']
    source_type = source_info['connection_type']
    target_type = target_info['connection_type']
    metaworkflow_id= data_json.get('id')
    print('-----source_type-----', source_type)
    print('-----target_type-----', target_type)
    print('-----table_name-----', table_name)
    list_of_constraint=[]
    created_by = target_info.get('createdBy')
    project = target_info.get('project')
    if source_type.lower() == 'oracle':
        sql_query = ddl_oracle_transformation(source_type, target_type, metadata, table_name)
        list_of_constraint=generate_oracle_alter_constarint_query(metadata, table_name)
    elif source_type.lower() == 'mssql':
        sql_query = ddl_mssql_transformation(source_type, target_type, metadata, table_name)
        
    elif source_type.lower() == 'mysql':
        sql_query = ddl_mysql_transformation(source_type, target_type, metadata, table_name)
    elif source_type.lower() == 'db2':
        sql_query = ddl_db2_transformation(source_type, target_type, metadata, table_name)
    elif source_type.lower() == 'postgres':
        sql_query = ddl_postgres_transformation(source_type, target_type, metadata, table_name)
    elif source_type.lower() == 'mongodb':
        sql_query = ddl_postgres_transformation(source_type, target_type, metadata, table_name)             
   
    execute_query_on_target(target_info, sql_query)
    execute_constraint_query_on_target(target_info,list_of_constraint)
    err, results =generate_metadata_details(target_info, table_name)
    for each in results:
         each["createdBy"]=  created_by
         each['project']= project
    if err:
        target_info['status']='inactive'
    else:
        target_info['status']='active'
        if len(results)==0:
             err, results =generate_metadata_details(target_info, table_name)
        update_connection_metadata_table(results,endpoint_connection_metadata)
        get_metadata_workflow_status_updated(metaworkflow_id,{ "Status": "Created"} ,endpoint_metadata)
    
    
    
    
    
    
    
    '''
    # resp=get_metadata_workflow_status_updated(metaworkflow_id,{ "Status": "Created"} ,endpoint_metadata)
    # print(resp)
    # old_table_name = data_json['old_table_name']
    # # print("---------------old Table name--------", old_table_name)
    # print('old_table_name', old_table_name)
    # if len(old_table_name)>0:
    #     result = delete_table_for_connection(target_connection, old_table_name)
    #     print("-----------result---------", result)
    #     delete_metadata_for_table(target_connection, old_table_name, endpoint_connection_metadata)

    
# with DAG(dag_id="ddl_migration", default_args=default_args, catchup=False) as dag:
   
#     read_input_json = PythonOperator(task_id='read_input_json', python_callable=read_input_json, provide_context=True, dag=dag, )
#     check_available_table_on_target = BranchPythonOperator(task_id='check_available_table_on_target',
#                                                            python_callable=check_available_table_on_target,
#                                                            provide_context=True, dag=dag, )

    # alter_table = PythonOperator(task_id='alter_table', python_callable=generate_alter_table_query, provide_context=True, dag=dag, )
    # create_table_query = PythonOperator(task_id='create_table_query', python_callable=generate_create_table_query,  provide_context=True, dag=dag, )

#     trigger = TriggerDagRunOperator( task_id="refresh_info", trigger_dag_id="Check_Connection_Status",  conf='{"connection_id":"{{ task_instance.xcom_pull(task_ids="read_input_json", key="target_connection")["connection_id"] }}"}')

#     read_input_json >> check_available_table_on_target >> [alter_table, create_table_query]
#     create_table_query>>trigger
'''