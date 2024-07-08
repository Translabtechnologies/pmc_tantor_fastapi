from model.schemas import MigrationDag,PurgeDag,DDLMigration,MetadataOrclMssqlDb2,CheckConnectionStatus,CheckConnectivity

from fastapi import Depends, HTTPException

from sqlalchemy.orm import Session
from fastapi import FastAPI
from db.database import engine
from db.database import SessionLocal
from model.model import metadata
from model import model
from datetime import datetime
import logging
import json
from tantor_lib.fetch_metadata import get_metadata_details
from tantor_lib.fetch_connection_status import fetch_connection_status
from tantor_lib.ddl_mig import ddl_dag, generate_alter_table_query,generate_create_table_query
from tantor_lib.metadata_detail.tantor_metadata_fetch import test_connectivity
logging.basicConfig(level=logging.DEBUG)



app=FastAPI()

metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def insert_values(api_name, input_data, created_at, db):
    try:
        db.execute(
            model.check_connection_status.insert().values(
                api_name=api_name,
                input_data=input_data,
                created_at=created_at,
            )
        )
        db.commit()
    except Exception as e:
        logging.error(f"An error occurred during insertion: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal Server Error")

def update_values(api_name, output_data, db):
    try:
        db.execute(
            model.check_connection_status.update()
            .where(model.check_connection_status.c.api_name == api_name)
            .values(
                output_data=output_data,
                updated_at=datetime.now(),
                status="completed"
            )
        )
        db.commit()
    except Exception as e:
        logging.error(f"An error occurred during update: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/migration_dag/")
async def create_migration_dag(migration_dag: MigrationDag, db: Session = Depends(get_db)):
    api_name = "migration_dag"
    input_data = json.dumps(migration_dag.dict())
    created_at = datetime.now()
    print((api_name, input_data, created_at))

    insert_values(api_name, input_data, created_at, db)

    # Perform business logic here
    output_data = "Some output data"

    update_values(api_name, output_data, db)

    return {"message": "Data received and stored successfully"}








@app.post("/purge_dag/")
async def create_purge_dag(purge_dag: PurgeDag, db: Session = Depends(get_db)):
    api_name = "purge_dag"
    input_data = json.dumps(purge_dag.dict())
    created_at = datetime.now()
    print((api_name, input_data, created_at))

    insert_values(api_name, input_data, created_at, db)

    # Perform business logic here
    output_data = "Some output data"

    update_values(api_name, output_data, db)

    return {"message": "Data received and stored successfully"}


@app.post("/ddl_migration/")
async def create_ddl_migration(ddl_migration:DDLMigration, db: Session = Depends(get_db)):
    api_name = "ddl_migration"
    input_data = ddl_migration.json()
    created_at = datetime.now()
    # print((api_name, input_data, created_at))
    insert_values(api_name, input_data, created_at, db)
    print('-------input_data--------',input_data)
    result = ddl_dag(input_data)
    # print('---result---------', result)
    if result == "alter_table" :
        print('hbshbsajhabsjhas',generate_alter_table_query(input_data))
    else:
        print('generate_table_query', generate_create_table_query(input_data))
    return {"message": "Data received and stored successfully"}



@app.post("/metadata_orcl_mssql_db2")
async def create_metadata_orcl_mssql_db2(metadata_orcl_mssql_db: MetadataOrclMssqlDb2, db: Session = Depends(get_db)):
    api_name = "metadata_orcl_mssql_db2"
    json_data = json.dumps(metadata_orcl_mssql_db.dict())
    input_data = json.loads(json_data)
    conn_info = input_data.get('connection_info')
    get_metadata_details(conn_info)
    return {"message": "Data received and stored successfully"}

# @app.post("/check_connection status")
# async def create_check_connection_status (check_connection_status:CheckConnectionStatus , db: Session = Depends(get_db)):
#     api_name = "metadata_orcl_mssql_db2"
#     json_str = json.dumps(check_connection_status.dict())
#     python_dict = json.loads(json_str)
#     created_at = datetime.now()
#     print(type(python_dict))

#     fetch_connection_status(input_data)

#     # Perform business logic here
#     output_data = "Some output data"

#     update_values(api_name, output_data, db)

#     return {"message": "Data received and stored successfully"}



@app.delete("/delete_purge_dag/")
async def create_purge_dag(purge_dag: PurgeDag, db: Session = Depends(get_db)):
    api_name = "purge_dag"
    input_data = json.dumps(purge_dag.dict())
    created_at = datetime.now()
    print((api_name, input_data, created_at))


    # Perform business logic here

    output_data = "Some output data"


    return {"message": "Data deleted successfully"}


@app.delete("/delete_migration_dag/")
async def create_ddl_migration(ddl_migration:DDLMigration, db: Session = Depends(get_db)):
    api_name = "ddl_migration"
    input_data = ddl_migration.json()
    created_at = datetime.now()
    print((api_name, input_data, created_at))

    # Perform business logic here
    output_data = "Some output data"

    return {"message": "Data deleted successfully"}


@app.post("/check_connectivity")
async def checkconnection(connection_info:CheckConnectivity, db: Session = Depends(get_db)):
    json_data = json.dumps(connection_info.dict())
    input_data = json.loads(json_data)
    conn_info = input_data.get('connection_info')
    status = test_connectivity(conn_info)
    return {"message": status}
