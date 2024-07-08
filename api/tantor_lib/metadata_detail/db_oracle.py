import oracledb

class OracleConectionManger:
    """
    this class for oracle database.
    """

    def __init__(self, connection_info) -> None:
        print("=====db======", connection_info)
        """
        initialize value connection details and create the connection with oracle database
        """
        self.host_address = connection_info.get('host_address', None)
        self.port_number = connection_info.get('port_number', None)
        self.database_name = connection_info.get('database_name', None)
        self.schema_name = connection_info.get('schema_name', None)
        self.username = connection_info.get('username', None)
        self.password = connection_info.get('password', None)
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            print(f"Connection creation geting error {err}")
            raise Exception(err)
        else:
            print("Connection has been created")
            
    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                'dsn': f'{self.host_address}:{self.port_number}/{self.database_name}',
                'user': self.username,
                'password': self.password}
            con = oracledb.connect(**params)
        except Exception as err:
            return err, con
        return err, con
