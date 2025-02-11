import uuid
import io
import datetime
import json
from typing import Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel
from minio import Minio
import os
import asyncio


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


buffer_flush_interval = 5 # Time interval in seconds between writes to minio
buffer = []
buffer_lock = asyncio.Lock()

async def flush_buffer_task():
    while True:
        await asyncio.sleep(buffer_flush_interval)
        async with buffer_lock:
            if buffer:
                await flush_buffer()

async def flush_buffer():
    global buffer
    if not buffer:
        return


    # Organize the events in the buffer by the prefix `name/version/year/month/day`
    events_by_prefix = {}
    for event in buffer:
        ts = datetime.datetime.fromisoformat(event.metadata.timestamp)
        prefix = f'{event.metadata.name}/{event.metadata.version}/{ts.year}/{ts.month}/{ts.day}'
        if prefix not in events_by_prefix:
            events_by_prefix[prefix] = []
        events_by_prefix[prefix].append(event)
    
    for prefix in events_by_prefix:
        id = uuid.uuid4().hex
        object_name = f'{prefix}/{id}.jsonl'

        events = events_by_prefix[prefix]

        # Create the jsonl contents of the file
        object_data_str = "\n".join(json.dumps(event.dict()) for event in events)
        object_data_bytes = object_data_str.encode('utf-8')
        object_data_stream = io.BytesIO(object_data_bytes)

        res = minio_client.put_object(
                bucket_name="raw",
                object_name=object_name,
                data=object_data_stream,
                length=len(object_data_bytes),
        )
        print(f'{len(events)} events written to: {res.bucket_name}/{res.object_name}')
    
    buffer = []

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(flush_buffer_task())

@app.post("/event")
async def foobar(event: Event) -> Dict[str, str]:
    async with buffer_lock:
        buffer.append(event)

    return {
        'status':'ok'
    }
