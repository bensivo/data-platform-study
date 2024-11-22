import uuid
import io
import datetime
import json
from typing import Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel
from minio import Minio
import os


minio_host = os.getenv('MINIO_HOST', "localhost")
minio_access_key = os.getenv('MINIO_ACCESS_KEY', "my-access-key")
minio_secret_key = os.getenv('MINIO_SECRET_KEY', "my-secret-key")

minio_client = Minio(
    endpoint=f'{minio_host}:9000',
    access_key=minio_access_key,
    secret_key=minio_secret_key,
    secure=False
)


app = FastAPI()

class EventMetadata(BaseModel):
    name: str
    version: str
    timestamp: str

class Event(BaseModel):
    metadata: EventMetadata
    payload: Dict[str, Any]

@app.post("/event")
def foobar(event: Event) -> Dict[str, str]:

    print(json.dumps(event.dict(), indent=2))

    event_json = json.dumps(event.dict())

    current_time = datetime.datetime.utcnow()
    year  = current_time.year
    month = current_time.month
    day   = current_time.day
    hour = current_time.hour
    minute = current_time.minute
    second = current_time.second

    time_str = current_time.strftime("%Y-%m-%dT%H-%M-%S") # NOTE: not ISO8601, becuase colons in filenames can mess with spark / hadoop

    id = uuid.uuid4().hex

    object_name = f'{event.metadata.name}/{event.metadata.version}/{year}/{month}/{day}/{time_str}_{id}.json'

    object_data_str = event_json
    object_data_bytes = object_data_str.encode('utf-8')
    object_data_stream = io.BytesIO(object_data_bytes)

    # TODO: Do batch uploads, instead of one file per event, use JSONL and store multiple events in one file
    res = minio_client.put_object(
            bucket_name="raw",
            object_name=object_name,
            data=object_data_stream,
            length=len(object_data_bytes),
    )
    print(f'File written to: {res.bucket_name}/{res.object_name}')

    return {
        'status':'ok'
    }


