
from io import BytesIO
from fastapi import FastAPI, Form, HTTPException, BackgroundTasks, UploadFile, File
from fastapi.responses import JSONResponse
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi
import re
import requests
import json
from fastapi.middleware.cors import CORSMiddleware
import httpx


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from interaction-service"}


@app.post('/upload')
async def upload(file: UploadFile = File(...), id: str = Form(...), background_tasks: BackgroundTasks = BackgroundTasks()):
    contents = await file.read()
    background_tasks.add_task(process_contents, contents, id)
    print(f"Received content {id}") # {contents}")
    return JSONResponse(status_code=200, content={"message": "File uploaded successfully"})


@app.get('/confirm')
async def confirm(id: str):
    # Call the db-handler-service GET endpoint with the 'id' parameter

    print(f"Received id {id}")

    async with httpx.AsyncClient() as client:
        response = await client.get(f"http://localhost:8005/get-data?id={id}")

        if response.status_code == 200:
            data = response.json()
            print(f"Response from db-handler-service: {data}")
            return {"data": data["data"]}

    # # data = response.json()["data"]
    # print(f"Response from db-handler-service: {response}")
    # print(f"Response from db-handler-service: {response.json()}")
    # return {"data": []}


def process_contents(contents, id):
    buffer = BytesIO(contents)
    df = pd.read_excel(buffer)
    print(f'Dataframe from frontend: {df}')
    first_column = df.iloc[:, 0].tolist()
    print(f' URLs from frontend : {first_column}')
    send_urls_to_api_service(first_column, id)


def send_urls_to_api_service(video_urls, id):
    url = 'http://localhost:8001/send-urls'

    payload = {
        'doc_id': id,
        'urls': video_urls,
    }
    payload_json = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, data=payload_json, headers=headers)
    print("Status Code:", response.status_code)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
    # uvicorn app:app --host 0.0.0.0 --port 8000 --reload
