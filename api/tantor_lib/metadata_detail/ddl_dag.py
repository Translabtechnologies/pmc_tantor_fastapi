from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from tantor.metadata_detail.tantor_metadata_fetch import get_metadata_workflow_status_updated
from tantor.ddl.ddl_convertor import check_table_on_target, execute_query_on_target, execute_constraint_query_on_target,get_ddl_transformation
from tantor.metadata_detail.tantor_metadata_fetch import process_connection_info
from tantor.migration.migration_lib import fetch_connection_info
from tantor.metadata_detail.dag_config import endpoint_connection, endpoint_connection_metadata, default_args, endpoint_metadata, endpoint_metadata_by_conn_name


def read_input_json(**kwargs):
    dag_run_conf = kwargs["dag_run"].conf
    source_conn_id= dag_run_conf.get('source_conn_id', None)
    target_conn_id= dag_run_conf.get('target_conn_id', None)
    source_conn_info= fetch_connection_info(source_conn_id, endpoint_connection)
    target_conn_info= fetch_connection_info(target_conn_id, endpoint_connection)    
    result = check_table_on_target(target_conn_info, dag_run_conf['target_table'])
    if result:
        kwargs['ti'].xcom_push(key='target_connection', value=target_conn_info)
        kwargs['ti'].xcom_push(key='source_connection', value=source_conn_info)
        kwargs['ti'].xcom_push(key='metadata', value=dag_run_conf.get('metadata', {}))
        kwargs['ti'].xcom_push(key='new_table_name', value=dag_run_conf['target_table'])
        kwargs['ti'].xcom_push(key='metaworkflow_id', value=dag_run_conf.get('metadata_id',None))
        kwargs['ti'].xcom_push(key='created_by', value=dag_run_conf.get('created_by'))
        kwargs['ti'].xcom_push(key='project', value=dag_run_conf.get('project'))
        return 'create_table_query'
    else:
        kwargs['ti'].xcom_push(key='metaworkflow_id', value=dag_run_conf.get('metadata_id',None))
        return 'alter_table'
    

def generate_alter_table_query(**kwargs):
    metaworkflow_id = kwargs['ti'].xcom_pull(key='metaworkflow_id', task_ids='read_input_json')
    get_metadata_workflow_status_updated(metaworkflow_id,{ "status": "Table already exists"} ,endpoint_metadata)


def generate_create_table_query(**kwargs):
    source_connection = kwargs['ti'].xcom_pull(key='source_connection', task_ids='read_input_json')
    target_connection = kwargs['ti'].xcom_pull(key='target_connection', task_ids='read_input_json')
    metadata = kwargs['ti'].xcom_pull(key='metadata', task_ids='read_input_json')
    table_name = kwargs['ti'].xcom_pull(key='new_table_name', task_ids='read_input_json')
    sql_query, list_of_constraint=get_ddl_transformation(source_connection['connection_type'], target_connection['connection_type'], metadata, table_name)
    execute_query_on_target(target_connection, sql_query)
    print("------------------Constraint-----------------------",list_of_constraint )
    execute_constraint_query_on_target(target_connection,list_of_constraint)
    metaworkflow_id = kwargs['ti'].xcom_pull(key='metaworkflow_id', task_ids='read_input_json')
    # connection status update 
    get_metadata_workflow_status_updated(metaworkflow_id,{ "status": "Created"} ,endpoint_metadata)
    process_connection_info(target_connection,endpoint_metadata_by_conn_name,endpoint_connection_metadata,endpoint_connection)
    if source_connection['connection_name']== target_connection['connection_name']:
            process_connection_info(source_connection,endpoint_metadata_by_conn_name,endpoint_connection_metadata,endpoint_connection)
    
with DAG(dag_id="ddl_migration", default_args=default_args, catchup=False) as dag:
    read_input_json = BranchPythonOperator(task_id='read_input_json', python_callable=read_input_json, provide_context=True, dag=dag, )
    alter_table = PythonOperator(task_id='alter_table', python_callable=generate_alter_table_query, provide_context=True, dag=dag, )
    create_table_query = PythonOperator(task_id='create_table_query', python_callable=generate_create_table_query,  provide_context=True, dag=dag, )
    read_input_json >> [alter_table, create_table_query]