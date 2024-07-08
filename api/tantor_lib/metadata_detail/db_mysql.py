import mysql.connector


class MySqlConectionManger:
    """
    this class for MySQL the connection and detail of database.
    """
    def __init__(self, connection_info):
        """
        initialize value connection details and create the connection with mssql database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '')
        self.database_name = connection_info.get('database_name', '')
        self.schema_name = connection_info.get('schema_name', '')
        self.username = connection_info.get('username', '')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print("Connection not created ")
            raise Exception(err)

    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                'host': self.host_address,
                'user': self.username,
                'password': self.password,
                'database': self.database_name,
                'port': self.port_number,
                'raise_on_warnings': True
            }
            print(params)
            con = mysql.connector.connect(**params)
            print("con-----------", con)
        except Exception as err:
            print(err)
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()

    