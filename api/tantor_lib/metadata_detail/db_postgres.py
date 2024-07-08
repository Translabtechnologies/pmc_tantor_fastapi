import psycopg2
from string import Template
import os
from logs.t_logging import logger


class PostgresConectionManger:
    """
    this class for postgres database.
    """

    def __init__(self, connection_info) -> None:
        """
        initalize value connection details and create the connection with oracle database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '5432')
        self.database_name = connection_info.get('database_name', '')
        self.user_name= connection_info.get('username','')
        self.schema_name = connection_info.get('schema_name', 'public')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            raise Exception(err)


    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                "host": self.host_address,
                "database": self.database_name,
                "user": self.user_name,
                "password": self.password,
                "port": self.port_number,  # Default is 5432 for Postgres SQL
                
            }
            con = psycopg2.connect(**params)
        except Exception as err:
            logger.error(f"can not connect to postgres beacuse of {err}")
            return err, con
        return err, con

    