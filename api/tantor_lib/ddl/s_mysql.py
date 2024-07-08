from tantor_lib.ddl.datatype_map.mysql_to_other_db_map import mysql_to_oracle_mapping, mysql_to_mssql_mapping, \
    mysql_to_db2_mapping, mysql_to_postgres_mapping


def ddl_mysql_transformation(source_type, target_type, source_metadata, table_name):
    """
    Generates a SQL CREATE TABLE query for a target database type by transforming metadata from a source MySQL database.

    Parameters:
        source_type (str): The source database type (e.g., 'mysql').
        target_type (str): The target database type (e.g., 'oracle', 'mssql', 'db2', 'postgres').
        source_metadata (dict): Metadata or JSON representing the source MySQL table structure details.
        table_name (str): The name of the table to be created in the target database.

    Returns:
        str: The SQL query string for creating the table in the target database.

    Raises:
        ValueError: If an error occurs during the conversion process or if the source or target database type is invalid.

    Note:
        - The function supports transformations between MySQL and other databases such as Oracle, MSSQL, DB2, and Postgres.
        - The conversion_mapping dictionary maps source and target database types to conversion functions.
        - If the source and target types are the same, it directly calls the function to generate the CREATE TABLE query.
        - If a conversion is required, it fetches the appropriate conversion mapper and applies it to the source metadata.
        - The converted metadata is then used to generate the CREATE TABLE query for the target database type.
        - Any conversion errors are caught and reported as ValueErrors.
    """

    conversion_mapping = {
        ('mysql', 'oracle'): mysql_to_oracle_mapping,
        ('mysql', 'mssql'): mysql_to_mssql_mapping,
        ('mysql', 'db2'): mysql_to_db2_mapping,
        ('mysql', 'postgres'): mysql_to_postgres_mapping,

    }

    source_type = source_type.lower()
    target_type = target_type.lower()

    if source_type == target_type:
        return mysql_generate_create_table_query(source_metadata, table_name)
    else:
        conversion_mapper = conversion_mapping.get((source_type, target_type))
        if conversion_mapper:
            try:
                converted_metadata = convert_mysql_to_other(source_metadata, conversion_mapper, table_name)

                return mysql_to_other_generate_create_table_query(converted_metadata, table_name, target_type)

            except Exception as e:
                raise ValueError(
                    f"An error occurred during conversion: {str(e)}")
        else:
            raise ValueError("Invalid source or target database type.")


def mysql_generate_create_table_query(source_metadata, table_name):
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
            data_type = column['DATA_TYPE'].upper()
            data_length = column.get('DATA_LENGTH', None)
            data_precision = column.get('DATA_PRECISION', None)
            data_scale = column.get('DATA_SCALE', None)
            is_nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL"

            column_type_str = data_type

            if data_length == 'null' or data_length is None:
                data_length = None
            else:
                data_length = int(data_length)
            if data_scale == 'null' or data_scale is None:
                data_scale = None
            else:
                data_scale = int(data_scale)
            if data_precision != 'null' and data_precision is not None:
                data_precision = int(data_precision)
            else:
                data_precision = None
            
            if data_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "RAW", "VARCHAR", "VARBINARY"] and data_length is not None:
                    column_type_str += f"({data_length})"
            else:
                    column_type_str += "(MAX)"
            
            if data_type in ["NUMBER", "DECIMAL", "FLOAT","NUMERIC"] and  data_precision is not None:
                if data_scale is not None:
                    column_type_str += f"({data_precision}, {data_scale})"
                else:
                    column_type_str += f"({data_precision})"

            create_table_query += f"\t`{column_name}` {column_type_str} {is_nullable},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n);"
        print('------create_table_query-------------', create_table_query)
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None


def convert_mysql_to_other(source_metadata, conversion_mapper, table_name):
    """
    Converts metadata from a source MySQL table to another database type using a specified conversion mapper.

    Parameters:
        source_metadata (dict): Metadata or JSON representing the source MySQL table structure details.
        conversion_mapper (dict): Mapping of MySQL data types to their corresponding types in the target database.
        table_name (str): The name of the table for which the metadata is being converted.

    Returns:
        dict: Transformed metadata with updated data types based on the specified conversion rules.

    Note:
        - The function iterates through columns in the source_metadata.
        - For each column, it extracts information such as data type, size, precision, and scale.
        - It handles cases where size, precision, or scale are missing or marked as 'null' in the source_metadata.
        - It then applies the conversion rules specified in the conversion_mapper to update the data type.
        - If a conversion function is provided for a specific data type, it is called with size, precision, and scale.
        - If a direct mapping is available, the data type is updated accordingly.
        - Any errors during the conversion process are caught and reported.

    Example:
        Given source_metadata with a column of type 'VARCHAR(50)' and a conversion_mapper mapping 'VARCHAR' to 'TEXT':
        Input: {'DATA_TYPE': 'VARCHAR', 'DATA_LENGTH': '50', ...}
        Output: {'DATA_TYPE': 'TEXT', 'DATA_LENGTH': 50, ...}
    """

    columns = source_metadata["metadata"][0].get("column_json", [])
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
                temp = conversion_mapper[column_type](size, precision, scale)
                column["DATA_TYPE"] = temp
            else:
                temp = conversion_mapper[column_type]
                column["DATA_TYPE"] = temp

    return source_metadata


def mysql_to_other_generate_create_table_query(source_metadata, table_name, target_type):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
        table_name:
        target_type:
        source_metadata (dict): Metadata or JSON representing the source table structure details.

    Returns:
        str: The SQL query string for creating the table.

    Note:
        The input `source_metadata` should contain information about the table's name, columns, data types,
        constraints, and other properties required for creating the table.
    """
    columns = source_metadata["metadata"][0].get("column_json", [])
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            is_nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL" 
            if target_type == 'mssql':
                create_table_query += f"\t{column_name} {data_type} {is_nullable},\n"
            else:
                create_table_query += f"\t{column_name} {data_type} {is_nullable},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        print('---------create_table_query---------', create_table_query)
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None
