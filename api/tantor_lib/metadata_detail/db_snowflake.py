import snowflake.connector

class SnowflakeConnectionManager:
    """
    This class manages connections to a Snowflake database.
    """

    def __init__(self, connection_info) -> None:
        """
        Initialize connection details and create the connection with Snowflake.
        """
        self.account = connection_info.get('host_address', '')
        self.role = connection_info.get('role', '')
        self.warehouse = connection_info.get('warehouse', '')
        self.user = connection_info.get('username', '')
        self.password = connection_info.get('password', '')
        self.database_name = connection_info.get('database_name', '').upper()
        self.source_schema = connection_info.get('source_schema', '').upper()
        err = self.create_connection()
        if err:
            raise Exception(f"Failed to connect to Snowflake: {err}")
        else:
            print('Connection successful.')  # Optional: You can keep or remove this line

    def create_connection(self):
        """Connect to the Snowflake database."""
        try:
            self.connection = snowflake.connector.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                role=self.role,
                warehouse=self.warehouse,
                database=self.database_name,
                schema=self.source_schema
            )
            return None
        except Exception as e:
            return e

    def metadata_details(self, table_name=None):
        """Fetches the metadata for a particular connection.
        
        Parameters
        ----------
        table_name : str
            Name of the table.
        
        Returns
        -------
        meta_data_details : list
            List containing all the details related to the table.
        """
        meta_data_details = []

        if table_name is None:
            sql_table = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.source_schema}' AND table_catalog = '{self.database_name}' AND table_type = 'BASE TABLE'"
            with self.connection.cursor() as cursor:
                cursor.execute(sql_table)
                for result in cursor.fetchall():
                    print('table_ai----------->')
                    temp = {
                        'table_schema': self.source_schema.lower(),
                        'table_name': result[0],
                        'column_detail': self.fetch_table_detail(result[0]),
                        'constraint_details': {},
                        'index_details': {},
                        'partition_json': {}
                    }
                    meta_data_details.append(temp)
        else:
            temp = {
                'table_schema': self.source_schema.lower(),
                'table_name': table_name,
                'column_detail': self.fetch_table_detail(table_name),
                'constraint_details': {},
                'index_details': {},
                'partition_json': {}
            }
            meta_data_details.append(temp)

        return meta_data_details

    def fetch_table_detail(self, table_name):
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
                sql_col_detail = f"""SELECT 
                                    column_name,
                                    data_type,
                                    character_maximum_length,
                                    numeric_precision,
                                    numeric_scale,
                                    is_nullable,
                                    column_default
                                FROM 
                                    information_schema.columns
                                WHERE 
                                    table_catalog = '{self.database_name}'
                                    AND table_schema = '{self.source_schema}'
                                    AND table_name = '{table_name}'
                                ORDER BY 
                                    ordinal_position;
                                """
                cursor.execute(sql_col_detail)
                column_details_list = cursor.fetchall()
        except Exception as e:
            print(f"Error fetching column details for {table_name}: {e}")
        
        temp_col = []
        for column_details in column_details_list:
            column_details = ['null' if each is None else each for each in column_details]
            temp_col.append({
                "column_name": column_details[0],
                "data_type": column_details[1],
                "character_maximum_length": column_details[2],
                "numeric_precision": column_details[3],
                "numeric_scale": column_details[4],
                "is_nullable": column_details[5],
                "column_default": column_details[6]
            })
        print('all_details---------->', temp_col)
        return temp_col

