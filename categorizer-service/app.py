
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request

import requests
import json


import pandas as pd
import random


__email_list = ["example1@email.com", "example2@email.com", "example3@email.com"]


app = FastAPI()


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from categorize-service"}


@app.post('/categorize')
async def make_transcript(request: Request, background_tasks: BackgroundTasks):
    # Parse the JSON body
    body = await request.json()

    # Access the metadata
    summaries = body.get('summaries')
    video_urls = body.get('video_urls')
    doc_id = body.get('doc_id')

    background_tasks.add_task(categorize_summaries, summaries, video_urls, doc_id)

    print(f"Received YouTube URL : {video_urls}") 
    return {"message": "Received YouTube URL"}


def categorize_summaries(summaries, video_urls, doc_id):
    categorized = _append_categories_to_summaries(summaries, video_urls)
    send_data_to_db_handler_service(categorized, doc_id)


def _append_categories_to_summaries(summaries, videos):
    df = _create_dataframe(summaries, videos)
    return _append_to_person(df)


def _create_dataframe(summaries, videos):
    return pd.DataFrame({
        'url': videos,
        'summary': summaries,
    })


def _append_to_person(df):
    df['email'] = df.apply(lambda row: _to_whom(row), axis=1)
    return df


def _to_whom(row):
    people_df = _read_people()
    belongs_to = random.randint(0, people_df.size-1)
    return people_df.iloc[belongs_to]['email']


def _read_people():
    # df = pd.read_excel('data/urls.xlsx', sheet_name='Sheet3', header=None)
    # df.columns = ['email']
    
    df = pd.DataFrame(__email_list, columns=['email'])

    return df



def send_data_to_db_handler_service(categorized, doc_id):
    url = 'http://localhost:8005/load-to-db'

    categorized_json = categorized.to_json(orient='split')

    payload = {
        'categorized': categorized_json,
        'doc_id': doc_id,
    }
    payload_json = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, data=payload_json, headers=headers)
    print("Status Code:", response.status_code)



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
