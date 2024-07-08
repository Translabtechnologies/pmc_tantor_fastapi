from tantor_lib.ddl.datatype_map.mssql_to_other_db_map import mssql_to_oracle_mapping, mssql_to_mysql_mapping, \
    mssql_to_db2_mapping, mssql_to_postgres_mapping


def ddl_mssql_transformation(source_type, target_type, source_metadata, table_name):
    """
    This function generates a SQL CREATE TABLE query for a target database type by transforming metadata from a source
    database type. It supports transformations between Microsoft SQL Server (MSSQL) and other databases such as Oracle,
    MySQL, DB2, and PostgreSQL.

    Parameters:
        source_type (str): The source database type (e.g., 'mssql').
        target_type (str): The target database type (e.g., 'oracle', 'mysql', 'db2', 'postgres').
        source_metadata (dict): Metadata or JSON representing the source table structure details.
        table_name (str): The name of the table to be created in the target database.

    Returns:
        str: The SQL query string for creating the table in the target database.

    Raises: ValueError: If an error occurs during the conversion process or if the source or target database type is
    invalid.

    Note: - The input source_metadata should contain information about the table's name, columns, data types,
    constraints, and other properties required for creating the table. - The conversion_functions dictionary maps
    source and target database types to conversion functions. - If the source and target types are the same,
    it directly calls the function to generate the CREATE TABLE query. - If a conversion is required, it fetches the
    appropriate conversion mapper and applies it to the source metadata. - The result is then used to generate the
    CREATE TABLE query for the target database type. - Any conversion errors are caught and reported as ValueErrors.
    """

    conversion_functions = {
        ('mssql', 'oracle'): mssql_to_oracle_mapping,
        ('mssql', 'mysql'): mssql_to_mysql_mapping,
        ('mssql', 'db2'): mssql_to_db2_mapping,
        ('mssql', 'postgres'): mssql_to_postgres_mapping,
    }
    source_type = source_type.lower()
    target_type = target_type.lower()

    if source_type == target_type:
        print('---source_type_and_target_type', source_type, target_type)
        return mssql_generate_create_table_query(source_metadata, table_name)
    else:
        conversion_mapper = conversion_functions.get((source_type, target_type))
        if conversion_mapper:
            try:
                return mssql_to_other_generate_create_table_query( convert_mssql_to_other(source_metadata, conversion_mapper), table_name, target_type)
            except Exception as e:
                raise ValueError(f"An error occurred during conversion: {str(e)}")
        else:
            raise ValueError("Invalid source or target database type.")


def convert_mssql_to_other(source_metadata, conversion_mapper):
    """
    Converts metadata from Microsoft SQL Server (MSSQL) to another database type using a specified conversion mapper.

    Parameters:
        source_metadata (dict): Metadata or JSON representing the source table structure details.
        conversion_mapper (dict): Mapping of MSSQL data types to their corresponding types in the target database.

    Returns:
        dict: Transformed metadata with updated data types based on the specified conversion rules.

    Note:
        - The function iterates through columns in the source_metadata.
        - For each column, it extracts information such as data type, size, precision, and scale.
        - It handles cases where size, precision, or scale are missing or marked as 'null' in the source_metadata.
        - It then applies the conversion rules specified in the conversion_mapper to update the data type.
        - If a conversion function is provided for a specific data type, it is called with size, precision, and scale.
        - If a direct mapping is available, the data type is updated accordingly.
        - If the column type is not found in the conversion_mapper, it prints a message indicating an invalid type.

    Example: Given source_metadata with a column of type 'NVARCHAR(50)' and a conversion_mapper mapping 'NVARCHAR' to
    'VARCHAR': Input: {'DATA_TYPE': 'NVARCHAR', 'DATA_LENGTH': '50', ...} Output: {'DATA_TYPE': 'VARCHAR',
    'DATA_LENGTH': 50, ...}
    """

    columns = source_metadata["metadata"][0].get("column_json", [])
    print('-------columns_mssql------------', columns)
    for column in columns:
        column_type = column.get("DATA_TYPE").upper()
        size = column.get("DATA_LENGTH")
        precision = column.get("DATA_PRECISION")
        scale = column.get("DATA_SCALE")
        if size == 'null' or size is None:
            size = 0
        else:
            size = int(size)
        if scale == 'null' or scale is None:
            scale = 0
        else:
            scale = int(scale)
        if precision != 'null' and precision is not None:
            precision = int(precision)
        else:
            precision = 0
        if column_type in conversion_mapper:
            if callable(conversion_mapper[column_type]):
                column["DATA_TYPE"] = conversion_mapper[column_type](size, precision, scale)
            else:
                column["DATA_TYPE"] = conversion_mapper[column_type]
        else:
            print(f"Invalid column type: {column_type}")

    return source_metadata

   
def mssql_generate_create_table_query(source_metadata, table_name):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
        table_name:
        source_metadata (dict): Metadata or JSON representing the source table structure details.

    Returns:
        str: The SQL query string for creating the table.

    Note:
        The input `source_metadata` should contain information about the table's name, columns, data types,
        constraints, and other properties required for creating the table.
    """
    columns = source_metadata["metadata"][0].get("column_json", [])
    print('-------columns--------', columns)
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"

        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            data_length = column.get('DATA_LENGTH')
            data_precision = column.get('DATA_PRECISION')
            data_scale = column.get('DATA_SCALE')
            is_nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL"
            if data_length == 'null':
                data_length=None
            if data_precision == 'null':
                data_precision=None
            if data_scale=='null':
                data_scale=None

            column_type_str = data_type

            if data_type.upper() in ["VARCHAR", "NVARCHAR", "CHAR", "BINARY", "VARBINARY"]:
                print("------------data_length-------------",type(data_length))
                if data_length is not None and data_length > 0:
                    column_type_str += f"({data_length})"
                else:
                    column_type_str += "(MAX)"
            if data_type.upper() in ["DECIMAL", "FLOAT" ,"NUMERIC"] and data_precision is not None:
                if data_scale is not None:
                    column_type_str += f"({data_precision}, {data_scale})"
                else:
                    column_type_str += f"({data_precision})"
            
            # create_table_query += f"\t{column_name} {column_type_str} {is_nullable} {default_clause},\n"
            create_table_query += f"\t{column_name} {column_type_str} {is_nullable},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        print('-----create_table_query--------', create_table_query)
        return create_table_query

    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None




def mssql_to_other_generate_create_table_query(source_metadata, table_name, target_type):
    """
    Generates a SQL CREATE TABLE query for a target database type based on metadata transformed from Microsoft SQL
    Server (MSSQL).

    Parameters:
        source_metadata (dict): Metadata or JSON representing the transformed source table structure details.
        table_name (str): The name of the table to be created in the target database.
        target_type (str): The target database type ('mysql', 'oracle', 'db2', 'postgres', etc.).

    Returns:
        str: The SQL query string for creating the table in the target database.

    Note:
        - The function takes transformed metadata from MSSQL to another database type.
        - It iterates through columns in the source_metadata to build the CREATE TABLE query.
        - For each column, it extracts information such as column name, data type, and nullability.
        - The is_nullable variable is determined based on the 'is_nullable' value in the source_metadata.
        - It generates the CREATE TABLE query with the appropriate syntax for the target database type.
        - If the target database type is MySQL, it uses backticks for column names; otherwise, it uses standard syntax.
        - Any errors during query generation are caught, and an error message is printed.

    Example:
        Given source_metadata for a VARCHAR column named 'example_column' with is_nullable='YES':
        Input: {'column_name': 'example_column', 'DATA_TYPE': 'VARCHAR', 'is_nullable': 'YES', ...}
        Output (for MySQL): 'CREATE TABLE table_name (\n`example_column` VARCHAR NULL\n)'
    """

    columns = source_metadata["metadata"][0].get("column_json", [])
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            is_nullable = ""
            if column.get("is_nullable") == "NO":
                is_nullable = "NOT NULL" 
            if target_type == 'mysql':
                create_table_query += f"\t`{column_name}` {data_type} {is_nullable},\n"
            elif target_type == 'oracle':
                create_table_query += f"\t{column_name} {data_type} {is_nullable},\n"
            elif target_type == 'db2':
                create_table_query += f"\t{column_name} {data_type} {is_nullable},\n"
            else:
                create_table_query += f"\t{column_name} {data_type} {is_nullable} \n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        print('-----------create_table_ query--------', create_table_query)
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None
