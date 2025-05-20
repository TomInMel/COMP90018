# -*- coding: utf-8 -*-
"""
Created on Tue May 20 05:28:05 2025

@author: Jyyyy
"""

from flask import Flask, request, jsonify
from typing import Dict, List, Optional
from elasticsearch import Elasticsearch
import pandas as pd
from dateutil.parser import parse
from datetime import datetime, timezone
import numpy as np
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


# Use the scroll to get all matching documents in batches from Elasticsearch
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


# platform-level summary
def platform_summary(index_name: str = "search_all",
                     start: Optional[str] = None,
                     end: Optional[str] = None,
                     quantiles=(0.1, 0.25, 0.5, 0.75, 0.9)):

    must = [{"term": {"type": "post"}}]
    if start or end:
        rng = {}
        if start: rng["gte"] = start
        if end:   rng["lte"] = end
        must.append({"range": {"created_utc": rng}})

    query = {"_source": ["platform", "sentiment_score"],
             "query": {"bool": {"must": must}}}

    df = pd.DataFrame(_scroll_all(index_name, query))
    if df.empty:
        raise ValueError("No post data for given parameters.")

    g = df.groupby("platform")["sentiment_score"]

    summary = (g.agg(["mean", "std", "size"])
                 .rename(columns={"mean": "avg",
                                  "std":  "std",
                                  "size": "count"})
                 .reset_index())

    # quantile
    for q in quantiles:
        summary[f"q{int(q*100)}"] = g.quantile(q).values

    return summary
    # return: platform, avg, std, count, q10, q25, q50, q75, q90

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
    
# rolling (single platform)
def rolling_health(index_name: str,
                   platform: str,
                   window_days: int = 7,
                   start: str = None,
                   end: str = None):
 
    # Return dataframe: date, rolling_avg, rolling_std (only for posts on the specified platform)

    must = [
        {"term": {"type": "post"}},
        {"term": {"platform": platform}}
    ]
    if start or end:
        rng = {}
        if start: rng["gte"] = start
        if end:   rng["lte"] = end
        must.append({"range": {"created_utc": rng}})

    query = {"_source": ["created_utc", "sentiment_score"],
             "query": {"bool": {"must": must}},
             "sort": [{"created_utc": "asc"}]}

    df = pd.DataFrame(_scroll_all(index_name, query))
    if df.empty:
        raise ValueError(f"No data for platform={platform}")
    
    df["created_utc"] = df["created_utc"].apply(parse_any_time)
    df["timestamp"]   = pd.to_datetime(df["created_utc"], utc=True)

    # df["timestamp"] = pd.to_datetime(df["created_utc"])
    daily = (df.set_index("timestamp")["sentiment_score"]
               .resample("D").mean())

    roll_avg = daily.rolling(window_days, min_periods=1).mean()
    roll_std = daily.rolling(window_days, min_periods=1).std(ddof=0)

    return pd.DataFrame({
        "date": roll_avg.index.date,
        "rolling_avg": roll_avg.values,
        "rolling_std": roll_std.values
    })
    
@app.route("/platform_summary", methods=["POST"])
def api_platform_summary():
    try:
        params = request.get_json()
        start = params.get("start")
        end = params.get("end")
        result = platform_summary(index_name="search_all", start=start, end=end)
        return jsonify(result.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/rolling_health", methods=["POST"])
def api_rolling_health():
    try:
        params = request.get_json()
        platform = params["platform"]
        start = params.get("start")
        end = params.get("end")
        window = int(params.get("window_days", 7))
        result = rolling_health(index_name="search_all", platform=platform,
                                start=start, end=end, window_days=window)
        return jsonify(result.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)