from pydantic import BaseModel, validator,Field
from typing import List, Optional, Dict, Union
from datetime import datetime




class Column(BaseModel):
    DATA_LENGTH: Optional[int]
    DATA_PRECISION: Optional[str]
    DATA_SCALE: Optional[str]
    DATA_TYPE: Optional[str]
    column_name: Optional[str]
    COLUMN_DEFAULT: Optional[str]
    is_nullable: Optional[str]


class Constrains(BaseModel):
    column_name: Optional[str]
    constraint_name: Optional[str]
    constraint_type: Optional[str]
    owner: Optional[str]
    position:Optional[str]
    search_condition:Optional[str]
    status:Optional[str]
    table_name:Optional[str]


class Metadata(BaseModel):
    column_json: List[Column] = Field(..., alias="column_json")
    conn_id: str
    conn_name: str
    conn_type: str
    constraint_json: Optional[list[Constrains]]
    extracted_at: datetime
    index_json: Dict = {}
    metadata_id: str
    partition_json: Dict = {}
    table_name: str

    class Config:
        allow_population_by_field_name = True


class Connection(BaseModel):
    connection_id: str
    connection_name: str
    connection_type: str
    database_name: str
    warehouse: Optional[str]
    role: Optional[str]
    host_address: Optional[str]
    password: str
    port_number: int
    source_schema: Optional[str]
    username: str
    created_by: str
    project: str

    @validator("*", pre=True)
    def check_empty_values(cls, value):
        if value is None or (isinstance(value, str) and value.strip() == ""):
            raise ValueError("Field cannot be null or empty")
        return value

class ScheduleInterval(BaseModel):
    interval_duration: str
    interval_type: str

class Scheduler(BaseModel):
    scheduleinterval: ScheduleInterval
    schedulerColumn: str
    startdatetime: str

class Source(BaseModel):
    iscomplexwherecondition: str
    sourceConnName: str
    sourceObjectName: str
    sourcePath: str
    sourceType: str
    sourceschema: List[str]
    wherecondition: List[str]

class LoadParams(BaseModel):
    changeCondition: str
    changeType: str
    checksumColumns: str
    loadType: str
    surrogateKey: str
    targetPk: str

class Partition(BaseModel):
    columns: List[str]
    hashColumn: str
    recNum: int
    strategy: str
    type: str


class DagConf(BaseModel):
    Name: str
    Project: str
    Remarks: str
    Status: str
    createdAt: datetime
    id: str
    target_connection: str
    target_table: str

class MigrationDag(BaseModel):
    created_by: Optional[str]
    project: str
    scheduler: Scheduler
    source: Source
    sourceConnection: Connection
    targetConnection: Connection
    transformation: List[dict]
    workflowName: str

class PurgeDag(BaseModel):
    created_by: Optional[str]
    project: str
    scheduler: Scheduler
    source: Source
    sourceConnection: Connection
    targetConnection: Connection
    transformation: List[dict]
    workflowName: str

class MetadataOrclMssqlDb2(BaseModel):
    connection_info: Connection

class DDLMigration(BaseModel):
    metadata: List[Metadata]
    old_table_name: str
    source: Connection
    table_name: str
    target: Connection
    Name: str
    Project: str
    Remarks: str
    Status: str
    createdAt: datetime
    id: str
    target_connection: str
    target_table: str

    class Config:
        allow_population_by_field_name = True



class CheckConnectionStatus(BaseModel):
    connection_id: str


class ConnInfo(BaseModel):
    connection_info: Connection


class CheckConnectivity(BaseModel):
    connection_info:Connection