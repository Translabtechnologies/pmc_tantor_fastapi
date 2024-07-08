import pymssql

class MsSqlConectionManger:
    """
    this class for MsSQL the connection and detail of databse.
    """

    def __init__(self, connection_info) -> None:
        print('---------class_conn-------',connection_info)
        """
        initalize value connection details and create the connection with mssql database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '1433')
        self.database_name = connection_info.get('database_name', '')
        self.username=connection_info.get('username','')
        self.schema_name = connection_info.get('schema_name', '')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        print('host',self.host_address, 'port',self.port_number,'database',self.database_name,'username',self.username,'password',self.password,'source_schema', self.source_schema)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print("Connection is failed ")
            raise Exception(err)
        else:
            print("Connection is establish ")

    def create_connection(self):
        """ Connect to the MSSQL server """
        con = None
        err = None
        try:
            params = {
                'server': self.host_address,
                'user': self.username,
                'password': self.password,
                'database': self.database_name,
                'port': self.port_number
            }
            con = pymssql.connect(**params)
        except Exception as err:
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()

    