# -*- coding: utf-8 -*-
"""
Created on Tue May 20 05:40:04 2025

@author: Jyyyy
"""

from flask import Flask, request, jsonify
from typing import Dict
from elasticsearch import Elasticsearch
import pandas as pd
import numpy as np
from gensim.utils import simple_preprocess
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import (CountVectorizer, TfidfVectorizer)
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

STOP = set(stopwords.words("english"))


## Use the scroll to get all matching documents in batches from Elasticsearch
def _scroll_all(index_name: str,
                query: Dict,
                batch_size: int = 5_000,
                scroll: str = "2m"):
    hits, resp = [], es.search(index=index_name, body=query,
                               size=batch_size, scroll=scroll)
    sid = resp["_scroll_id"]; hits.extend(resp["hits"]["hits"])
    while True:
        resp = es.scroll(scroll_id=sid, scroll=scroll)
        new = resp["hits"]["hits"]
        if not new:
            break
        hits.extend(new)
    return [h["_source"] for h in hits]

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
        dt = val
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

    else:
        raise ValueError(f"Unsupported time format: {val}")

    return dt.replace(microsecond=0).isoformat() if dt is not None else None

# 
def hot_terms(index_name: str,
              start: str,
              end: str,
              platform: str = "all",
              *,
              top_n: int = 20,
              ngram: int = 1,            # unigram OR bigram OR trigram
              method: str = "freq",      # "freq" OR "tfidf"
              sentiment_slice: str = None   # None OR "pos" OR "neg"
              ) :

# sentiment_slice : Take the upper quartile ("pos") / lower quartile ("neg") / full amount (None) of sentiment
    start = parse_any_time(start)
    end  = parse_any_time(end)

    must = [{"term": {"type": "post"}},
            {"range": {"created_utc": {"gte": start, "lte": end}}}]
    if platform != "all":
        must.append({"term": {"platform": platform}})

    query = {"_source": ["content", "sentiment_score"],
             "query": {"bool": {"must": must}}}

    docs = _scroll_all(index_name, query)
    if not docs:
        raise ValueError("No posts for given parameters")

    df = pd.DataFrame(docs)

    # optional sentiment slice 
    if sentiment_slice in ("pos", "neg"):
        q = 0.75 if sentiment_slice == "pos" else 0.25
        thr = df["sentiment_score"].quantile(q)
        df = df[df["sentiment_score"] >= thr] if sentiment_slice == "pos" \
             else df[df["sentiment_score"] <= thr]

    texts = [" ".join([t for t in simple_preprocess(c, deacc=True)
                       if t not in STOP])
             for c in df["content"].astype(str)]

    if method == "freq":
        vec = CountVectorizer(stop_words="english",
                              ngram_range=(ngram, ngram),
                              min_df=2)
        X = vec.fit_transform(texts)
        scores = np.asarray(X.sum(axis=0)).ravel()

    elif method == "tfidf":
        vec = TfidfVectorizer(stop_words="english",
                              ngram_range=(ngram, ngram),
                              min_df=2)
        X = vec.fit_transform(texts)
        scores = np.asarray(X.mean(axis=0)).ravel()
    else:
        raise ValueError("method must be 'freq' or 'tfidf'")

    terms = vec.get_feature_names_out()
    pairs = sorted(zip(terms, scores), key=lambda t: t[1], reverse=True)
    return pairs[:top_n]

def convert_np(obj):
    if isinstance(obj, (np.integer, np.int_, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float_, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

@app.route("/scenario5", methods=["POST"])
def scenario5_handler():
    try:
        params = request.get_json()

        index = params.get("index_name", "search_all")
        start = params["start"]
        end = params["end"]
        platform = params.get("platform", "all")
        top_n = int(params.get("top_n", 20))
        ngram = int(params.get("ngram", 1))
        method = params.get("method", "freq")
        sentiment_slice = params.get("sentiment_slice", None)

        results = hot_terms(index, start, end,
                    platform=platform,
                    top_n=top_n,
                    ngram=ngram,
                    method=method,
                    sentiment_slice=sentiment_slice)

        if isinstance(results, pd.DataFrame):
            results = results.astype(object).where(pd.notnull(results), None)
            results = results.to_dict(orient="records")

        if isinstance(results, list) and all(isinstance(r, tuple) and len(r) == 2 for r in results):
            results = [{"term": r[0], "score": convert_np(r[1])} for r in results]

        elif isinstance(results, list) and all(isinstance(r, dict) for r in results):
            results = [{k: convert_np(v) for k, v in row.items()} for row in results]

        return jsonify(results)


    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)