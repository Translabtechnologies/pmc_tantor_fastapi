from tantor_lib.ddl.datatype_map.oracle_to_other_db_map import oracle_to_mssql_mapping, oracle_to_mysql_mapping, \
    oracle_to_db2_mapping, oracle_to_postgres_mapping         


def ddl_oracle_transformation(source_type, target_type, source_metadata, table_name):
    """
    Transforms Oracle table metadata for creating a table in a different database type.

    Parameters:
        source_type (str): The source database type (e.g., 'oracle').
        target_type (str): The target database type to which the metadata will be transformed (e.g., 'mssql').
        source_metadata (dict): Metadata or JSON representing the source Oracle table structure details.
        table_name (str): The name of the table for which the metadata is being transformed.

    Returns:
        str: SQL query for creating a table in the target database with transformed metadata.

    Note:
        - The function first checks if the source and target database types are the same. If so, it generates the
          CREATE TABLE query using the original Oracle metadata.
        - If the source and target types differ, it looks up the appropriate conversion mapper for the transformation.
        - It then calls the convert_oracle_to_other function to apply the transformation to the metadata.
        - Finally, it generates the CREATE TABLE query using the transformed metadata.

    Example:
        Given source_metadata with a column of type 'NUMBER(10,2)' and target_type 'mssql':
        Input: ('oracle', 'mssql', {'column_json': [{'DATA_TYPE': 'NUMBER', 'DATA_PRECISION': '10', 'DATA_SCALE': '2'}, ...]}, 'example_table')
        Output: 'CREATE TABLE example_table (\n\tcolumn1 DECIMAL(10, 2),\n\t...);'
    """
    conversion_mapping = {
        ('oracle', 'mssql'): oracle_to_mssql_mapping,
        ('oracle', 'mysql'): oracle_to_mysql_mapping,
        ('oracle', 'db2'): oracle_to_db2_mapping,
        ('oracle', 'postgres'): oracle_to_postgres_mapping,   
    }

    source_type = source_type.lower()
    target_type = target_type.lower()

    if source_type == target_type:
        return oracle_generate_create_table_query(source_metadata, table_name)
    else:
        conversion_mapper = conversion_mapping.get((source_type, target_type))
        if conversion_mapper:
            try:
                converted_metadata=convert_oracle_to_other(source_metadata, conversion_mapper)
                return oracle_to_other_generate_create_table_query(converted_metadata, table_name, target_type)   
            except Exception as e:
                raise ValueError(f"An error occurred during conversion: {str(e)}")
        else:
            raise ValueError("Invalid source or target database type.")



def oracle_generate_create_table_query(source_metadata, table_name):
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
    columns = source_metadata.get("column_json", [])
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            data_length = column.get('DATA_LENGTH', None)
            data_precision = column.get('DATA_PRECISION', None)
            data_scale = column.get('DATA_SCALE', None)
            is_nullable = "NOT NULL" if column.get("is_nullable") == "N" else ''
            default_value = column.get("COLUMN_DEFAULT", None)
            column_type_str = data_type
            if default_value.strip() == 'null':
                default_value = ""
            if data_length == 'null':
                data_length = None
            else:
                data_length = int(data_length)
            if data_scale == 'null':
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
            
            if data_type in ["NUMBER","DECIMAL", "FLOAT","NUMERIC"] and data_precision is not None:
                if data_scale is not None and data_scale !=0:
                    column_type_str += f"({data_precision}, {data_scale})"
                else:
                    column_type_str += f"({data_precision})"

            create_table_query += f"\t{column_name} {column_type_str} {is_nullable} {default_value},\n"  

        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:
        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None

    



def convert_oracle_to_other(source_metadata, conversion_mapper):
    """
    Converts metadata from a source Oracle table to another database type using a specified conversion mapper.

    Parameters:
        source_metadata (dict): Metadata or JSON representing the source Oracle table structure details.
        conversion_mapper (dict): Mapping of Oracle data types to their corresponding types in the target database.

    Returns:
        dict: Transformed metadata with updated data types based on the specified conversion rules.

    Note:
        - The function iterates through columns in the source_metadata.
        - For each column, it extracts information such as data type, size, precision, and scale.
        - It handles cases where size, precision, or scale are missing or marked as 'null' in the source_metadata.
        - It then applies the conversion rules specified in the conversion_mapper to update the data type.
        - If a conversion function is provided for a specific data type, it is called with size, precision, and scale.
        - If a direct mapping is available, the data type is updated accordingly.
        - If the column type is not present in the conversion_mapper, an error message is printed.

    Example:
        Given source_metadata with a column of type 'VARCHAR2(50)' and a conversion_mapper mapping 'VARCHAR2' to 'TEXT':
        Input: {'DATA_TYPE': 'VARCHAR2', 'DATA_LENGTH': '50', ...}
        Output: {'DATA_TYPE': 'TEXT', 'DATA_LENGTH': 50, ...}
    """
    columns = source_metadata.get("column_json", [])

    for column in columns:
        column_type = column.get("DATA_TYPE")
        size = column.get("DATA_LENGTH")
        precision = column.get("DATA_PRECISION")
        scale = column.get("DATA_SCALE")
        if size=='null' or size is None:
            size=0
        else:
            size=int(size)
        if scale == 'null' or scale is None:
            scale = 0
        else:
            scale=int(scale)
        if precision != 'null' and precision is not None:
            precision = int(precision)
        else:
            precision = 0
        if column_type in conversion_mapper:
            if callable(conversion_mapper[column_type]):
                temp= conversion_mapper[column_type](size, precision, scale)
                column["DATA_TYPE"] = temp
            else:
                temp = conversion_mapper[column_type]
                column["DATA_TYPE"] = temp

        else:
            print(f"Invalid column type: {column_type}")
           
    return source_metadata




def oracle_to_other_generate_create_table_query(source_metadata, table_name, target_type):
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
    print("oracle_generate_create_table_query  ---------- called")
    columns = source_metadata.get("column_json", [])
    try:
        create_table_query = f"CREATE TABLE {table_name} (\n"
        for column in columns:
            column_name = column['column_name']
            data_type = column['DATA_TYPE']
            is_nullable = "NULL" if column.get("is_nullable") == "Y" else "NOT NULL" 
            # default_value = column.get("COLUMN_DEFAULT", None)
            if target_type == 'mysql':                                       
                create_table_query += f"\t`{column_name}` {data_type} {is_nullable},\n"
            else:
                create_table_query += f"\t{column_name} {data_type} {is_nullable},\n" 
        create_table_query = create_table_query.rstrip(",\n")
        create_table_query += "\n)"
        return create_table_query
    except Exception as e:
        print("An error occurred while generating the CREATE TABLE query:", str(e))
        return None



def generate_oracle_alter_constarint_query(metadata, table_name):
    data= metadata.get('constraint_json')
    list_of_constarint=[]
    primary_constarint=[]
    consistancy_constraint=[]
    uniquekey_constraint=[]
    if len(data)>0:
        for each_cont in data:
            print("--------------------------")
            if each_cont['constraint_type']=='P':
                primary_constarint.append(each_cont)
            elif each_cont['constraint_type'].rstrip()=='C':
                consistancy_constraint.append(each_cont)
            elif each_cont['constraint_type']=='U':
                uniquekey_constraint.append(each_cont)
        if primary_constarint:
            primary_cont_query=f'''ALTER TABLE  {table_name} ADD CONSTRAINT {table_name}_{primary_constarint[0]['column_name']}_pk PRIMARY KEY ('''
            for index,each_cont in enumerate(primary_constarint):
                if index<len(primary_constarint)-1:
                    primary_cont_query+=f'''{each_cont['column_name']},'''
                else:
                    primary_cont_query+=f'''{each_cont['column_name']})'''
            list_of_constarint.append(primary_cont_query)
        if uniquekey_constraint:
            for _,each_cont in enumerate(uniquekey_constraint):
                list_of_constarint.append(f"alter table  {table_name} add constraint {table_name}_{each_cont['column_name']}_uu UNIQUE ( {each_cont['column_name']})")
        if consistancy_constraint:
            for each in consistancy_constraint:
                list_of_constarint.append(f"ALTER TABLE {table_name} MODIFY {each['column_name']} CONSTRAINT {each['column_name']}_NN NOT NULL")

    return list_of_constarint