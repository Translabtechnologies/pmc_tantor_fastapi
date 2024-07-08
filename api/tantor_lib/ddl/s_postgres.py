
from tantor_lib.ddl.datatype_map.postgres_to_other_db_map import postgres_to_mssql_mapping, postgres_to_mysql_mapping, \
    postgres_to_db2_mapping, postgres_to_oracle_mapping


def ddl_postgres_transformation(source_type, target_type, source_metadata, table_name):
    """
    Transforms PostgreSQL table metadata for creating a table in a different database type.

    Parameters:
        source_type (str): The source database type (e.g., 'postgres').
        target_type (str): The target database type to which the metadata will be transformed (e.g., 'oracle').
        source_metadata (dict): Metadata or JSON representing the source PostgreSQL table structure details.
        table_name (str): The name of the table for which the metadata is being transformed.

    Returns:
        str: SQL query for creating a table in the target database with transformed metadata.

    Note:
        - The function first checks if the source and target database types are the same. If so, it generates the
          CREATE TABLE query using the original PostgreSQL metadata.
        - If the source and target types differ, it looks up the appropriate conversion mapper for the transformation.
        - It then calls the convert_postgres_to_other function to apply the transformation to the metadata.
        - Finally, it generates the CREATE TABLE query using the transformed metadata.

    Example:
        Given source_metadata with a column of type 'integer' and target_type 'mssql':
        Input: ('postgres', 'mssql', {'column_json': [{'DATA_TYPE': 'integer'}, ...]}, 'example_table')
        Output: 'CREATE TABLE example_table (\n\tcolumn1 INT,\n\t...);'
    """

    conversion_mapping = {
        ('postgres', 'oracle'): postgres_to_oracle_mapping,
        ('postgres', 'mssql'): postgres_to_mssql_mapping,
        ('postgres', 'db2'): postgres_to_db2_mapping,
        ('postgres', 'mysql'): postgres_to_mysql_mapping,
    }

    source_type = source_type.lower()
    target_type = target_type.lower()

    if source_type == target_type:
        return postgres_generate_create_table_query(source_metadata, table_name)
    else:

        conversion_mapper = conversion_mapping.get((source_type, target_type))

        if conversion_mapper:
            try:
                converted_metadata = convert_postgres_to_other(
                    source_metadata, conversion_mapper)

                return postgres_to_other_generate_create_table_query(converted_metadata, table_name, target_type)
            except Exception as e:
                raise ValueError(
                    f"An error occurred during conversion: {str(e)}")
        else:
            raise ValueError("Invalid source or target database type.")


def postgres_generate_create_table_query(source_metadata, table_name):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
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
            data_length = column.get('DATA_LENGTH', None)
            data_precision = column.get('DATA_PRECISION', None)
            data_scale = column.get('DATA_SCALE', None)
            is_nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL"
            

            if data_type== 'timestamp without time zone':
                column_type_str = 'timestamp'
            elif data_type== 'timestamp with time zone':
                column_type_str = 'timestamptz'
            else:
                column_type_str=data_type
           

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
            if data_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "RAW"]:
                if data_length is not None:
                    column_type_str += f"({data_length})"
                else:
                    column_type_str += "(MAX)"
            if data_type == ["NUMBER","DECIMAL", "FLOAT", "NUMERIC"] and data_precision is not None:  
                if data_scale is not None:
                    column_type_str += f"({data_precision}, {data_scale})"
                else:
                    column_type_str += f"({data_precision})"
            create_table_query += f"\t{column_name} {column_type_str} {is_nullable},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None


def convert_postgres_to_other(source_metadata, conversion_mapper):
    """
    Converts PostgreSQL table metadata to a different database type using a specified conversion mapper.

    Parameters:
        source_metadata (dict): Metadata or JSON representing the source PostgreSQL table structure details.
        conversion_mapper (dict): Mapping of PostgreSQL data types to their corresponding types in the target database.

    Returns:
        dict: Transformed metadata with data types updated based on the conversion_mapper.

    Note:
        - The function iterates through each column in the source_metadata.
        - For each column, it extracts information such as data type, size, precision, and scale.
        - It checks if the data type is present in the conversion_mapper.
        - If a mapping exists, it applies the conversion using the provided function or directly assigns the new data type.
        - The updated metadata is then returned.

    Example:
        Given source_metadata with a column of type 'integer' and a conversion_mapper for 'postgres' to 'mssql':
        Input: ({'column_json': [{'DATA_TYPE': 'integer'}, ...]}, {'integer': 'INT'})
        Output: {'column_json': [{'DATA_TYPE': 'INT'}, ...]}
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
        else:
            print(f"Invalid column type: {column_type}")
    return source_metadata


def postgres_to_other_generate_create_table_query(source_metadata, table_name, target_type):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
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
            is_nullable = "NULL" if column.get("is_nullable") == "YES" else "NOT NULL" if column.get("is_nullable") == "NO" else "NULL"
            default_value = column.get("COLUMN_DEFAULT", None)
            
            if default_value is not None:
                default_clause = f" DEFAULT {default_value}"
                is_nullable = "NOT NULL"
            else:
                default_clause = ""
            if target_type == 'mysql':
                create_table_query += f"\t`{column_name}` {data_type} {is_nullable} {default_clause},\n"
            elif target_type == 'oracle':
                create_table_query += f"\t{column_name} {data_type} {default_clause} {is_nullable},\n"
            elif target_type == 'db2':
                create_table_query += f"\t{column_name} {data_type} {default_clause} {is_nullable},\n"
            else:
                create_table_query += f"\t{column_name} {data_type} {is_nullable} {default_clause},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None






