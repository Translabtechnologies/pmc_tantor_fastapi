import socket
import os 
import platform
if platform.system() == "Windows":
    os.add_dll_directory(r'D:\ntx64_odbc_cli\clidriver\bin')
import ibm_db


class Db2ConnectionManager():
    """A simple class that manages a Db2 server or database connection."""
    def __init__(self,connection_info):
        """Initialize Db2 server or database name, user ID, and password attributes."""

        self.ds_type = connection_info.get('dsType', 'DB')
        self.host_address=connection_info.get('host_address','')
        self.port_number=connection_info.get('port_number',50000)
        self.database_name=connection_info.get('database_name','')
        self.schema_name=connection_info.get('schema_name','')
        self.username=connection_info.get('username','')
        self.password=connection_info.get('password','')
        self.source_schema=connection_info.get('source_schema','')
        err, self.connection = self.create_connection()
        if self.connection is None :
             raise Exception(err)
        
        print('Connection successful')
        
    def create_connection(self):
        """Attempt to establish a Db2 server or database connection."""
        # Define And Initialize The Appropriate Local Variables
        conn_string = "DRIVER={IBM DB2 ODBC DRIVER}"
        connectionID=None
        msg_string=''
        conn_string += ";DATABASE=" + self.database_name  # Only Used To Connect To A Database
        conn_string += ";HOSTNAME=" + str(self.host_address)  # Only Used To Connect To A Server
        conn_string += ";PORT=" + str(self.port_number)  # Only Used To Connect To A Server
        conn_string += ";PROTOCOL=TCPIP"
        conn_string += ";UID=" + self.username
        conn_string += ";PWD=" + self.password
        print('-----------------conn_string-------------' ,conn_string)
        try:
            connectionID = ibm_db.connect(conn_string, self.username, self.password)
        except Exception:
            pass
        if connectionID is None:
            msg_string = ibm_db.conn_errormsg()
        return msg_string, connectionID
    

    def connection_close(self):
        """Attempt to close a Db2 server or database connection."""
        msg_string = ""
        return_code = True
        if self.connection is not None:
            try:
                return_code = ibm_db.close(self.connection)
            except Exception:
                pass
            if return_code is False:
                msg_string = ibm_db.conn_errormsg(self.connection)
                return_code = False
            else:
                return_code = True
        return return_code, msg_string


    