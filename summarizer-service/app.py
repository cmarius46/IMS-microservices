
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from youtube_transcript_api import YouTubeTranscriptApi
import re
import requests
import json


import nltk 
from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize, sent_tokenize 
    


app = FastAPI()


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from summarizer-service"}


@app.post('/summarize')
async def summarize(request: Request, background_tasks: BackgroundTasks):
    # Parse the JSON body
    body = await request.json()

    # Access the metadata
    transcripts = body.get('transcripts')
    video_urls = body.get('video_urls')
    doc_id = body.get('doc_id')

    background_tasks.add_task(summarize_transcripts, transcripts, video_urls, doc_id)

    print(f"Received YouTube URL : {video_urls}") # {metadata}
    return {"message": "Received YouTube URL"}


def summarize_transcripts(transcripts, video_urls, doc_id):
    summary_list = []
    for transcript in transcripts:
        summary = _summarize_text(transcript)
        summary_list.append(summary)
    send_data_to_categorizer_service(summary_list, video_urls, doc_id)


def _summarize_text(text):
    # Input text - to summarize  
    
    # Tokenizing the text 
    stopWords = set(stopwords.words("english")) 
    words = word_tokenize(text) 
    
    # Creating a frequency table to keep the  
    # score of each word 
    
    freqTable = dict() 
    for word in words: 
        word = word.lower() 
        if word in stopWords: 
            continue
        if word in freqTable: 
            freqTable[word] += 1
        else: 
            freqTable[word] = 1
    
    # Creating a dictionary to keep the score 
    # of each sentence 
    sentences = sent_tokenize(text) 
    sentenceValue = dict() 
    
    for sentence in sentences: 
        for word, freq in freqTable.items(): 
            if word in sentence.lower(): 
                if sentence in sentenceValue: 
                    sentenceValue[sentence] += freq 
                else: 
                    sentenceValue[sentence] = freq 
    
    sumValues = 0
    for sentence in sentenceValue: 
        sumValues += sentenceValue[sentence] 
    
    # Average value of a sentence from the original text 
    
    average = int(sumValues / len(sentenceValue)) 
    
    # Storing sentences into our summary. 
    summary = '' 
    for sentence in sentences: 
        if (sentence in sentenceValue) and (sentenceValue[sentence] > (0.9 * average)): 
            summary += " " + sentence 
    
    return summary


def send_data_to_categorizer_service(summary_list, video_urls, doc_id):
    url = 'http://localhost:8004/categorize'

    payload = {
        'summaries': summary_list,
        'video_urls': video_urls,
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
    uvicorn.run(app, host="0.0.0.0", port=8003)
