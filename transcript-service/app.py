
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from youtube_transcript_api import YouTubeTranscriptApi
import re
import requests
import json


app = FastAPI()


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from transcript-service"}


@app.post('/make-transcript')
async def make_transcript(request: Request, background_tasks: BackgroundTasks):
    # Parse the JSON body
    body = await request.json()

    # Access the metadata
    metadatas = body.get('metadatas')
    video_urls = body.get('video_urls')
    doc_id = body.get('doc_id')

    background_tasks.add_task(process_metadatas, metadatas, video_urls, doc_id)

    print(f"Received YouTube URL : {video_urls}") # {metadata}
    return {"message": "Received YouTube URL"}


def send_data_to_summarizer_service(full_text_list, video_urls, doc_id):
    url = 'http://localhost:8003/summarize'

    payload = {
        'transcripts': full_text_list,
        'video_urls': video_urls,
        'doc_id': doc_id,
    }
    payload_json = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, data=payload_json, headers=headers)
    print("Status Code:", response.status_code)


def process_metadatas(metadatas, video_urls, doc_id):
    full_text_list = []
    for metadata in metadatas:
        full_text = _make_full_text_from_transcript(metadata)
        full_text_list.append(full_text)

    send_data_to_summarizer_service(full_text_list, video_urls, doc_id)


def _make_full_text_from_transcript(transcript):
    if not transcript:
        return None
    return ' '.join(obj['text'] for obj in transcript)






if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
