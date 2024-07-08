# DEVELOPER BY GEETANSHU/NEHA/YOGESH SHARMA

oracle_to_mssql_mapping = {
    "VARCHAR2": lambda size, precision, scale: f"VARCHAR({size})" if size else "VARCHAR",
    "NVARCHAR2": lambda size, precision, scale: f"NVARCHAR({size})" if size else "NVARCHAR",
    "NUMBER": lambda size, precision, scale: (
        f"TINYINT({size})" if 0 < size < 3 else
        "SMALLINT" if 3 < size <= 5 else
        "INTEGER" if 5 < size <= 10 else
        "BIGINT" if size else f"DECIMAL({precision},{scale})"
    ),
    "BINARY_FLOAT": lambda size, precision, scale: f"FLOAT({size})",
    "BINARY_DOUBLE": lambda size, precision, scale: f"FLOAT({size})",
    "FLOAT": lambda size, precision, scale: f"FLOAT({precision})" if 0 < precision <= 53 else "FLOAT",
    "INTEGER": "INTEGER",
    "SMALLINT": "SMALLINT",
    "DECIMAL": lambda size, precision, scale: f"DECIMAL({precision},{scale})",
    "NUMERIC": lambda size, precision, scale: f"DECIMAL({precision},{scale})",
    "CHAR": lambda size, precision, scale: f"CHAR({size})" if size else "CHAR",
    "NCHAR": lambda size, precision, scale: f"NCHAR({size})" if size else "NCHAR",
    "CLOB": lambda size, precision, scale: f"NVARCHAR({size})",
    "NCLOB": lambda size, precision, scale: f"NVARCHAR({size})",
    "DATE": "DATETIME",
    "TIMESTAMP(6)": "DATETIME2",
    "BOOLEAN": "BIT",
    "BLOB": lambda size, precision, scale: f"VARBINARY({size})",
    "YEAR": "INT",
    "LONG": "TEXT",
    "REAL": "FLOAT",
    "RAW": lambda size, precision, scale: f"VARBINARY({size})" if size else "VARBINARY",
    "XMLTYPE": "XML",
}

oracle_to_db2_mapping = {
    "VARCHAR2": lambda size, precision, scale: f"VARCHAR({size})" if size else "VARCHAR",
    "NVARCHAR2": lambda size, precision, scale: f"NVARCHAR({size})" if size else "NVARCHAR",
    "NUMBER": lambda size, precision, scale: (f"DECIMAL({size})" if size else f"DECIMAL({precision},{scale})" if precision and scale else "DECIMAL"),
    "BINARY_FLOAT": "REAL",
    "BINARY_DOUBLE": "DOUBLE",
    "FLOAT": lambda size, precision, scale: ( "REAL" if precision and precision <= 24 else f"DOUBLE({precision},{scale})"),
    "INTEGER": "INTEGER",
    "SMALLINT": "SMALLINT",
    "NUMERIC": "DECIMAL",
    "CHAR": lambda size, precision, scale: f"CHAR({size})" if size else "CHAR",
    "NCHAR": lambda size, precision, scale: f"NCHAR({size})" if size else "NCHAR",
    "RAW": lambda size, precision, scale: f"VARBINARY({size})",
    "CLOB": "CLOB",
    "NCLOB": "DBCLOB",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "BOOLEAN": "SMALLINT",
    "BLOB": "BLOB",
    "YEAR": "INT",
    "LONG": "TEXT",
    "REAL": "REAL",
    "XMLTYPE": "XML",
}

oracle_to_mysql_mapping = {
    "VARCHAR2": lambda size, precision, scale: f"VARCHAR({size})" if size else "VARCHAR(MAX)",
    "NVARCHAR2": lambda size, precision, scale: f"NVARCHAR({size})" if size else "NVARCHAR(MAX)",
    "NUMBER": lambda size, precision, scale: (
        f"TINYINT" if size and precision and precision < 3 and scale == 0 else
        f"SMALLINT" if size and precision and precision < 5 and scale == 0 else
        f"INT" if size and precision and precision < 9 and scale == 0 else
        f"BIGINT" if size and precision and precision < 19 and scale == 0 else
        f"DECIMAL({precision},{scale})" if (precision or scale) else
        f"DOUBLE" if not (precision or scale) else
        "INT"
    ),
    "BINARY_FLOAT": lambda size, precision, scale: "FLOAT",
    "BINARY_DOUBLE": lambda size, precision, scale: "DOUBLE PRECISION",
    "FLOAT": lambda size, precision, scale: "DOUBLE",
    "INTEGER": lambda size, precision, scale: "INT",
    "SMALLINT": lambda size, precision, scale: "SMALLINT",
    "XMLTYPE": "LONGTEXT",
    "DECIMAL": lambda size, precision, scale: (
        f"DECIMAL({precision},{scale})" if precision and scale else
        "DECIMAL"
    ),
    "NUMERIC": lambda size, precision, scale: (
        f"NUMERIC({precision},{scale})" if precision and scale else
        "NUMERIC"
    ),
    "CHAR": lambda size, precision, scale: f"CHAR({size})" if size else "CHAR",
    "NCHAR": lambda size, precision, scale: (
        f"NCHAR({size})" if size and size < 255 else
        f"NVARCHAR({size})" if size else "NCHAR"
    ),
    "CLOB": lambda size, precision, scale: "LONGTEXT",
    "NCLOB": lambda size, precision, scale: "TEXT",
    "DATE": lambda size, precision, scale: "DATE",
    "TIMESTAMP": "DATETIME",
    "BLOB": "LONGBLOB",
    "BOOLEAN": "BIT",
    "YEAR": lambda size, precision, scale: "INT",
    "LONG": lambda size, precision, scale: "LONGTEXT",
    "LONG RAW": lambda size, precision, scale: "LONGBLOB",
    "REAL": lambda size, precision, scale: "DOUBLE",
    "DOUBLE PRECISION": lambda size, precision, scale: "DOUBLE PRECISION",
    "RAW": lambda size, precision, scale: (
        f"BINARY({size})" if size and size < 256 else
        f"VARBINARY({size})" if size else "RAW"
    )
}

oracle_to_postgres_mapping = {
    "VARCHAR2": lambda size, precision, scale: f"VARCHAR({size})" if size else "VARCHAR",
    "NVARCHAR2": lambda size, precision, scale: f"VARCHAR({size})" if size else "VARCHAR",
    "NUMBER": lambda size, precision, scale: (
        f"NUMERIC({precision}, {scale})"
        if precision != 0 and scale != 0
        else "NUMERIC"
    ),
    "BINARY_FLOAT": lambda size, precision, scale: "REAL",
    "BINARY_DOUBLE": lambda size, precision, scale: "DOUBLE PRECISION",
    "FLOAT": lambda size, precision, scale: (
        "DOUBLE PRECISION"
        if precision > 24
        else f"REAL({precision})"
        if precision > 0
        else "REAL"
    ),
    "INTEGER": lambda size, precision, scale: "INTEGER",
    "SMALLINT": lambda size, precision, scale: "SMALLINT",
    "XMLTYPE": "XML",
    "DECIMAL": lambda size, precision, scale: f"NUMERIC({precision}, {scale})",
    "NUMERIC": lambda size, precision, scale: f"NUMERIC({precision}, {scale})",
    "CHAR": lambda size, precision, scale: f"CHAR({size})" if size else "CHAR",
    "NCHAR": lambda size, precision, scale: f"CHAR({size})" if size else "CHAR",
    "CLOB": lambda size, precision, scale: "TEXT",
    "NCLOB": lambda size, precision, scale: "TEXT",
    "DATE": lambda size, precision, scale: "TIMESTAMP",
    "TIMESTAMP": lambda size, precision, scale: "TIMESTAMP",
    "BOOLEAN": lambda size, precision, scale: "BOOLEAN",
    "BLOB": lambda size, precision, scale: f"BYTEA" if size else "BYTEA",
    "LONG": lambda size, precision, scale: "TEXT",
    "REAL": lambda size, precision, scale: "REAL",
    "RAW": "BYTEA",
}
