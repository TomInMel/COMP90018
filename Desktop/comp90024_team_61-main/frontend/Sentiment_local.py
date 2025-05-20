# -*- coding: utf-8 -*-

import requests
import pandas as pd
import re
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline


# 1. fetch the row data
def fetch_data_from_api(index_name, base_url="http://localhost:8080/search_posts"):
    try:
        response = requests.get(base_url, params={"index": index_name})
        response.raise_for_status()
        docs = response.json()
        df = pd.DataFrame(docs)
        return df
    except Exception as e:
        print(f"Error fetching data from API for index '{index_name}': {e}")
        return pd.DataFrame()


# 2. Standard timestampt
def ensure_timestamp_format(df, column="created_utc"):
    df[column] = pd.to_datetime(df[column], errors='coerce')
    return df

# 3. input of sentiment analysis
def build_sentiment_input(row, index_type="post"):
    if index_type == "post":
        return f"{row.get('title', '')} {row.get('content', '')}".strip()
    elif index_type == "comment":
        return row.get("content", "")
    else:
        return ""

# 4. text cleaning
def clean_text(text: str):
    text = re.sub(r"http\S+|www\S+", "", text)      
    text = re.sub(r"@\w+", "", text)                 
    text = re.sub(r"#\w+", "", text)
    text = re.sub(r"\s+", " ", text).strip()         
    return text

# 5. Sentiment analysis
model_name = "cardiffnlp/twitter-roberta-base-sentiment-latest"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)
classifier = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

def get_compound_score_full_text(text: str, max_tokens=512): 
    # Tokenize but don't truncate yet
    tokens = tokenizer.tokenize(text)

    # Separate tokens into chunks
    chunks = [tokens[i:i + max_tokens] for i in range(0, len(tokens), max_tokens)]
    scores = []

    for chunk in chunks:
        chunk_text = tokenizer.convert_tokens_to_string(chunk)
        inputs = tokenizer(chunk_text, return_tensors="pt", truncation=True, max_length=max_tokens)
        outputs = model(**inputs)
        probs = outputs.logits.softmax(dim=1).detach().numpy()[0]

        label_map = model.config.id2label
        score_map = dict(zip([label_map[i].lower() for i in range(len(probs))], probs))

        if "positive" in score_map:
            score = +score_map["positive"]
        elif "negative" in score_map:
            score = -score_map["negative"]
        else:
            score = 0.0

        scores.append(score)

    return sum(scores) / len(scores) if scores else 0.0

# 6 
def write_data_to_es_via_api(df, index_name, api_url="http://localhost:8080/insert"):
    for _, row in df.iterrows():
        data = {
            "index": index_name,
            "doc": row.to_dict()
        }
        try:
            response = requests.post(api_url, json=data)
            if response.status_code not in [200, 201]:
                print(f"Failed to write document to {index_name}: {response.status_code} - {response.text}")
        except Exception as e:
            print(f"Exception during API write: {e}")

# 7. 
def run_clean_pipeline(raw_index, new_index, index_type="post", api_base_url="http://localhost:8080"):

    df = fetch_data_from_api(raw_index, base_url=f"{api_base_url}/search_posts")

    df = ensure_timestamp_format(df, column="created_utc")

    df['text_to_analyze'] = df.apply(lambda row: build_sentiment_input(row, index_type), axis=1)
    df['cleaned_text'] = df['text_to_analyze'].apply(clean_text)
    df['score'] = df['cleaned_text'].apply(get_compound_score_full_text)

    write_data_to_es_via_api(df, index_name=new_index, api_url=f"{api_base_url}/insert")

    print(f"Cleaned and scored data written to: {new_index}")
    
    
'''  
# Example in notebook
## post：title + content
run_clean_pipeline(
    raw_index="reddit_posts",
    cleaned_index="reddit_posts_sentiment",
    index_type="post"
)

## comment：content
run_clean_pipeline(
    raw_index="reddit_comments",
    cleaned_index="reddit_comments_sentiment",
    index_type="comment"
)  
'''