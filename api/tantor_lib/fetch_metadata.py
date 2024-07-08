from tantor_lib.metadata_detail.tantor_metadata_fetch import generate_metadata_details, get_metadata_table_list, update_connection_metadata_table, update_connection_status
from tantor_lib.enpoint_list import endpoint_list
from tantor_lib.enp_dcp import MY_KEY, decrypt
import json
from tantor_lib.metadata_detail.tantor_metadata_fetch import generate_metadata_details, get_metadata_table_list, update_connection_metadata_table, update_connection_status,delete_metadata_for_table
from tantor_lib.enpoint_list import endpoint_list
from tantor_lib.enp_dcp import MY_KEY, decrypt
from tantor_lib.metadata_detail.tantor_metadata_fetch import process_connection_info

def get_metadata_details(connection_info):
    '''        
    '''
    temp = {}
    endpoint_connection_metadata =endpoint_list.get('endpoint_connection_metadata')
    endpoint_connection=endpoint_list.get('endpoint_connection')
    endpoint_metadata_by_conn_name = endpoint_list.get('endpoint_metadata_connname')

    process_connection_info(connection_info,endpoint_metadata_by_conn_name,endpoint_connection_metadata,endpoint_connection)