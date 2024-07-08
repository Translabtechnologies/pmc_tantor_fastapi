from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, DateTime

metadata = MetaData()


check_connection_status = Table(
    'API',
    metadata,
    Column('api_name', String, index=True),
    Column('input_data', String),
    Column('output_data', String),
    Column('created_at', DateTime),
    Column('updated_at', DateTime),
    Column('status', String),
)