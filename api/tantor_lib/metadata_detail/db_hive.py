from pyhive import hive
from logs.t_logging import logger

class HiveConnectionManager:
    """
    This class manages connections to a Hive database and fetches metadata details.
    """

    def __init__(self, connection_info) -> None:
        """
        Initialize connection details and create the connection with Hive.
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '10000')  
        self.database_name = connection_info.get('database_name', '')
        self.user_name = connection_info.get('username', '')
        self.password = connection_info.get('password', '')
        self.auth_mode = 'LDAP' if self.password else 'NOSASL' 
        self.connection = None
        err = self.create_connection()
        if err:
            raise Exception(f"Failed to connect to Hive: {err}")
        else:
            logger.info('Connection successful.')

    def create_connection(self):
        """Connect to the Hive database server."""
        try:
            self.connection = hive.Connection(
                host=self.host_address,
                port=self.port_number,
                username=self.user_name,
                database=self.database_name,
                password=self.password if self.auth_mode == 'LDAP' else None,
                auth=self.auth_mode
            )
            return None
        except Exception as e:
            logger.error(f"Failed to connect to Hive: {e}")
            return e

    def metadata_details(self, table_name=None):
        """
        Fetch metadata details for a specific table or all tables in the database.
        
        Parameters
        ----------
        table_name : str, optional
            Name of the table to fetch metadata for. If None, fetch metadata for all tables.

        Returns
        -------
        list
            List containing metadata details for tables.
        """
        meta_data_details = []
        
        if table_name is None:
            sql_table = f"SHOW TABLES IN {self.database_name}"
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(sql_table)
                    for result in cursor.fetchall():
                        print('table_name------->', result[0])
                        table_name = result[0]
                        temp = {
                            'table_schema': self.database_name,
                            'table_name': table_name,
                            'column_detail': self.fetch_table_details(table_name),
                            'constraint_details': {},
                            'index_details': {},
                            'partition_json': {}
                        }
                        meta_data_details.append(temp)
            except Exception as e:
                logger.error(f"Error fetching tables: {e}")
        else:
            temp = {
                'table_schema': self.database_name,
                'table_name': table_name,
                'column_detail': self.fetch_table_details(table_name),
                'constraint_details': {},
                'index_details': {},
                'partition_json': {}
            }
            meta_data_details.append(temp)

        return meta_data_details

    def fetch_table_details(self, table_name):
        """
        Fetch column details for a specific table.
        
        Parameters
        ----------
        table_name : str
            Name of the table to fetch column details for.

        Returns
        -------
        list
            List of dictionaries containing column details.
        """
        column_details_list = []
        
        try:
            with self.connection.cursor() as cursor:
                sql_col_detail = f"DESCRIBE {self.database_name}.{table_name}"
                cursor.execute(sql_col_detail)
                column_details_list = cursor.fetchall()
        except Exception as e:
            logger.error(f"Error fetching column details for {table_name}: {e}")
        
        temp_col = []
        for column_details in column_details_list:
            column_details = ['null' if each is None else each for each in column_details]
            temp_col.append({
                "column_name": column_details[0],
                "data_type": column_details[1]
            })
        print(temp_col)
        
        return temp_col
