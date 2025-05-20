# -*- coding: utf-8 -*-
"""
Created on Tue May 20 05:14:27 2025

@author: Jyyyy
"""

from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timezone
import os

app = Flask(__name__)

es = Elasticsearch(
    hosts=["https://elasticsearch-master.elastic.svc.cluster.local:9200"],
    basic_auth=(os.environ.get("ES_USERNAME"), os.environ.get("ES_PASSWORD")),
    verify_certs=False,
    ssl_show_warn=False,
    headers={
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
    }
)


def _scroll_all(index_name, query, batch_size=5000, scroll="2m"):
    docs, resp = [], es.search(index=index_name, body=query, size=batch_size, scroll=scroll)
    sid = resp["_scroll_id"]; docs.extend(resp["hits"]["hits"])
    while True:
        resp = es.scroll(scroll_id=sid, scroll=scroll)
        hits = resp["hits"]["hits"]
        if not hits:
            break
        docs.extend(hits)
    return [h["_source"] for h in docs]

def parse_any_time(val):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        ts = val / 1000 if val > 1e12 else val
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    elif isinstance(val, str):
        dt = parse(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    elif isinstance(val, datetime):
        dt = val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    else:
        raise ValueError(f"Unsupported time format: {val}")
    return dt.replace(microsecond=0).isoformat()

def trump_daily_sentiment(index_name, start, end, platform="all"):
    from datetime import datetime

    must = [
        {"range": {"created_utc": {"gte": start, "lte": end}}}
    ]
    if platform != "all":
        must.append({"term": {"platform": platform}})
    if index_name == "reddit_posts_scored_fixed":
        must.insert(0, {"term": {"type": "posts"}})
        sentiment_field = "bertweet_sentiment"
    else:
        must.insert(0, {"terms": {"type": ["post", "comment"]}})
        sentiment_field = "sentiment_score"

    query = {
        "_source": ["created_utc", sentiment_field],
        "query": {"bool": {"must": must}}
    }

    docs = _scroll_all(index_name, query)
    if not docs:
        raise ValueError("No data found.")

    df = pd.DataFrame(docs)

    # Unwrap list values
    for col in ["created_utc", sentiment_field, "platform", "type"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x[0] if isinstance(x, list) else x)

    # Try to parse date strings
    df["created_utc"] = df["created_utc"].apply(parse_any_time)
    df["timestamp"] = pd.to_datetime(df["created_utc"], utc=True)
    df.rename(columns={sentiment_field: "score"}, inplace=True)
    df["date"] = df["timestamp"].dt.date

    daily = (df.groupby("date")["score"]
               .mean()
               .reset_index()
               .rename(columns={"score": "avg_score"}))
    return daily



@app.route("/scenario3", methods=["POST"])
def scenario3_endpoint():
    try:
        params = request.get_json()
        index = params.get("index_name", "trump_posts_scored")
        start = params.get("start")
        end = params.get("end")
        platform = params.get("platform", "all")
        result = trump_daily_sentiment(index, start, end, platform)
        return result.to_json(orient="records", date_format="iso")
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
