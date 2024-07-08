from tantor_lib.enpoint_list import endpoint_list
from tantor_lib.enp_dcp import MY_KEY, decrypt
import json
from tantor_lib.metadata_detail.tantor_metadata_fetch import process_connection_info
from tantor_lib.enpoint_list import endpoint_list
from tantor_lib.metadata_detail.tantor_metadata_fetch import test_connectivity

endpoint_metadata_by_conn_name = endpoint_list.get('endpoint_metadata_connname')
endpoint_connection_metadata = endpoint_list.get('endpoint_connection_metadata')
endpoint_connection = endpoint_list.get('endpoint_connection')
endpoint_metadata_by_conn_name = endpoint_list.get('endpoint_metadata_connname')

def fetch_connection_status(connection_info):
    '''        
    ''' 
    import ast
    conn_info=ast.literal_eval(connection_info)
    process_connection_info(conn_info,endpoint_metadata_by_conn_name,endpoint_connection_metadata,endpoint_connection)
    


  