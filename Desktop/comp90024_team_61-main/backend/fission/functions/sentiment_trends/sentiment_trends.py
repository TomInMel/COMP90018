import logging
import json
from flask import current_app, request
from elasticsearch8 import Elasticsearch
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime, timezone
from dateutil.parser import parse



es_client: Elasticsearch = Elasticsearch(
    'https://elasticsearch-master.elastic.svc.cluster.local:9200',
    verify_certs=False,
    ssl_show_warn=False,
    basic_auth=('elastic', 'elastic')
)


def _scroll_all(index_name: str,
                query: dict,
                batch_size: int = 5_000,
                scroll: str = "2m") -> List[dict]:
    docs, resp = [], es_client.search(index=index_name, body=query,
                               size=batch_size, scroll=scroll)
    sid = resp["_scroll_id"]; docs.extend(resp["hits"]["hits"])
    while True:
        resp = es_client.scroll(scroll_id=sid, scroll=scroll)
        hits = resp["hits"]["hits"]
        if not hits:
            break
        docs.extend(hits)
    return [h["_source"] for h in docs]
    # Returns a list of _sources that satisfy query

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

def _topk_post_ids(index_name: str, start: str, end: str,
                   k: int, platform: str) -> List[str]:
    must = [
        {"term": {"type": "comment"}},
        {"range": {"created_utc": {"gte": start, "lte": end}}}
    ]
    if platform != "all":
        must.append({"term": {"platform": platform}})

    body = {
        "size": 0,
        "query": {"bool": {"must": must}},
        "aggs": {
            "top_posts": {
                "terms": {"field": "post_id", "size": k,
                          "order": {"_count": "desc"}}
            }
        }
    }
    resp = es_client.search(index=index_name, body=body)
    return [b["key"] for b in resp["aggregations"]["top_posts"]["buckets"]]


def sentiment_trends(index_name: str,
                     start: str,
                     end: str,
                     k: int = 5,
                     platform: str = "all"):

    top_ids = _topk_post_ids(index_name, start, end, k, platform)
    if not top_ids:
        raise ValueError("There is no comment data under the specified conditions: top-K list is empty")

    must = [
        {"term": {"type": "comment"}},
        {"terms": {"post_id": top_ids}},
        {"range": {"created_utc": {"gte": start, "lte": end}}}
    ]
    if platform != "all":
        must.append({"term": {"platform": platform}})

    q_comment = {
        "_source": ["post_id", "created_utc", "sentiment_score"],
        "query": {"bool": {"must": must}},
        "sort": [{"created_utc": "asc"}]
    }

    df = pd.DataFrame(_scroll_all(index_name, q_comment))
    if df.empty:
        raise ValueError("Failed to fetch comments or field name does not match")

    df.rename(columns={"created_utc": "timestamp",
                       "sentiment_score": "score"}, inplace=True)
    df["timestamp"] = df["timestamp"].apply(parse_any_time)
    # df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df[["post_id", "timestamp", "score"]]
    # return dataframe(post_id, timestamp[pandas.Timestamp], score)



def main() -> Dict[str, Any]:
    try:

        req = request
        start = req.headers.get("X-Fission-Params-Start")
        end = req.headers.get("X-Fission-Params-End")
        k = int(req.headers.get("X-Fission-Params-K", 5))
        platform = req.headers.get("X-Fission-Params-Platform", "all")
        index = req.headers.get("X-Fission-Params-Index", "search_all")

        df = sentiment_trends(index_name=index, start=start, end=end, k=k, platform=platform)

        return {
                "code": 200,
                "data": df.to_dict(orient="records"),
                "meta": {
                    "start": start,
                    "end": end,
                    "k": k,
                    "platform": platform,
                    "index": index
                }
        }
    
    except Exception as e:
        current_app.logger.exception("Error in sentiment_trends")
        return {"error": str(e)}, 500



   