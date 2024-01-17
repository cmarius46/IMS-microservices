
from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from sqlalchemy import create_engine, text

import requests
import json


import pandas as pd
import random


engine = create_engine('sqlite:///mydb.db')


app = FastAPI()


@app.get('/sanity-check')
async def sanity_check():
    return {"message": "Hello from categorize-service"}


@app.post('/load-to-db')
async def make_transcript(request: Request, background_tasks: BackgroundTasks):
    # Parse the JSON body
    body = await request.json()

    # Access the metadata
    categorized = body.get('categorized')
    doc_id = body.get('doc_id')

    background_tasks.add_task(send_to_db, categorized, doc_id)

    print(f"Received YouTube URL : {categorized}") 
    return {"message": "Received YouTube URL"}


@app.get('/get-data')
async def get_data(id: str):
    with engine.connect() as connection:
        raw_sql_query = text("SELECT * FROM my_table WHERE doc_id = :doc_id")
        result = connection.execute(raw_sql_query, {'doc_id': id})

        column_names = result.keys()

        data_list = [{column: value for column, value in zip(column_names, row)} for row in result.fetchall()]
        # result = connection.execute(text("SELECT * FROM my_table WHERE doc_id = :id"), {'id': id})
        # data = [dict(row) for row in result]
        # print('-----data-----')
        # print(data)
        result.close()
        return {"data": data_list}


def send_to_db(categorized, doc_id):
    categorized_df = pd.read_json(categorized, orient='split')
    
    categorized_df['doc_id'] = doc_id

    categorized_df.to_sql('my_table', con=engine, if_exists='append', index=False)
    print("Successfully written to db")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)
