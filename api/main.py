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
from fastapi.middleware.cors import CORSMiddleware
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



origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.post("/metadata_orcl_mssql_db2")
async def create_metadata_orcl_mssql_db2(metadata_orcl_mssql_db: MetadataOrclMssqlDb2, db: Session = Depends(get_db)):
    json_data = json.dumps(metadata_orcl_mssql_db.dict())
    input_data = json.loads(json_data)
    conn_info = input_data.get('connection_info')
    get_metadata_details(conn_info)
    return {"message": "Dag run successful"}



@app.post("/check_connectivity")
async def checkconnection(connection_info:CheckConnectivity, db: Session = Depends(get_db)):
    json_data = json.dumps(connection_info.dict())
    input_data = json.loads(json_data)
    conn_info = input_data.get('connection_info')
    status = test_connectivity(conn_info)
    print(status)
    if status is None:
        return {"message": "Connection_successful"}
    else:
        return {"message": "Connection_Failed"}
