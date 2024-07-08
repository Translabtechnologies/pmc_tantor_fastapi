from tantor_lib.ddl.datatype_map.db2_to_other_db_map import db2_to_oracle_mapping, db2_to_mysql_mapping, \
    db2_to_mssql_mapping, db2_to_postgres_mapping


def ddl_db2_transformation(source_type, target_type, source_metadata, table_name):
    conversion_mapping = {
        ('db2', 'mssql'): db2_to_mssql_mapping,
        ('db2', 'mysql'): db2_to_mysql_mapping,
        ('db2', 'oracle'): db2_to_oracle_mapping,
        ('db2', 'postgres'): db2_to_postgres_mapping,
    }

    source_type = source_type.lower()
    target_type = target_type.lower()

    if source_type == target_type:
        return db2_generate_create_table_query(source_metadata, table_name)
    else:
        conversion_mapper = conversion_mapping.get((source_type, target_type))
        if conversion_mapper:
            try:
                converted_metadata = convert_db2_to_other(
                    source_metadata, conversion_mapper)

                return db2_to_other_generate_create_table_query(converted_metadata, table_name, target_type)

            except Exception as e:
                raise ValueError(
                    f"An error occurred during conversion: {str(e)}")
        else:
            raise ValueError("Invalid source or target database type.")


def db2_generate_create_table_query(source_metadata, table_name):
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
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            data_length = column.get('DATA_LENGTH', None)
            data_precision = column.get('DATA_PRECISION', None)
            data_scale = column.get('DATA_SCALE', None)

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
            
            if data_type in ["VARCHAR2", "NVARCHAR2", "CHAR", "RAW", "VARCHAR", "CHARACTER", "VARGRAPHIC", "VARBINARY"]:
                if data_length is not None:
                    column_type_str += f"({data_length})"
                else:
                    column_type_str += "(MAX)"
            
            if data_type in ["NUMBER","DECIMAL", "FLOAT", "NUMERIC"]  and data_precision is not None:
                if data_scale is not None:
                    column_type_str += f"({data_length}, {data_scale})"
                else:
                    column_type_str += f"({data_length})"
                    
            create_table_query += f"\t{column_name} {column_type_str} ,\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None


def convert_db2_to_other(source_metadata, conversion_mapper):
    columns = source_metadata["metadata"][0].get("column_json", [])

    for column in columns:
        column_type = column.get("DATA_TYPE")
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
        print("column_type", column_type, "size", size, "precision", precision, "scale", scale)
        if column_type in conversion_mapper:
            if callable(conversion_mapper[column_type]):
                temp = conversion_mapper[column_type](size, precision, scale)

                column["DATA_TYPE"] = temp
            else:
                temp = conversion_mapper[column_type]

                column["DATA_TYPE"] = temp

    return source_metadata


def db2_to_other_generate_create_table_query(source_metadata, table_name, target_type):
    """
    This function generates a SQL CREATE TABLE query based on the provided table metadata.

    Parameters:
        target_type:
        table_name:
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

            if target_type == 'mysql':
                create_table_query += f"\t`{column_name}` {data_type} ,\n"
            else:
                create_table_query += f"\t{column_name} {data_type},\n"

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:

        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None
