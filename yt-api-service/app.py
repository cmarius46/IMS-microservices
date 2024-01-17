
from fastapi import FastAPI, HTTPException, BackgroundTasks
from youtube_transcript_api import YouTubeTranscriptApi
import re
import requests
import json


app = FastAPI()


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from yt-api-service"}


@app.post('/send-urls')
async def send_url(data: dict, background_tasks: BackgroundTasks):
    urls = data.get("urls")
    doc_id = data.get("doc_id")
    background_tasks.add_task(process_urls, urls, doc_id)
    print(f"Received YouTube URLs {urls}")
    return {"message": "Received YouTube URL"}


def process_urls(urls: list, doc_id: str):
    metadata_list = get_video_metadata_from_youtube(urls)
    if metadata_list:
        print("Video metadata found")
        send_metadatas_to_transcripter_service(metadata_list, urls, doc_id)
    else:
        print("Error getting metadata")


def send_metadatas_to_transcripter_service(metadatas, video_urls, doc_id):
    url = 'http://localhost:8002/make-transcript'

    payload = {
        'metadatas': metadatas,
        'video_urls': video_urls,
        'doc_id': doc_id,
    }
    payload_json = json.dumps(payload)
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, data=payload_json, headers=headers)
    print("Status Code:", response.status_code)


def _get_video_id(url):
    # Extract the video ID from the URL using a regular expression
    match = re.search(r'(?<=v=)[^&]+', url)
    if match:
        video_id = match.group()
    else:
        # If no match is found, return None or raise an exception
        video_id = None
    return video_id


def get_video_metadata_from_youtube(video_urls):
    metadata_list = []

    for video_url in video_urls:
        video_id = _get_video_id(video_url)
        try:
            metadata = YouTubeTranscriptApi.get_transcript(video_id)
        except Exception as e:
            print(f'get_video_metadata_from_youtube EXCEPTION : {e}')
            metadata = None
        metadata_list.append(metadata)
    return metadata_list


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
    # uvicorn app:app --host 0.0.0.0 --port 8001 --reload
