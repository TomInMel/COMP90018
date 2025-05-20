# -*- coding: utf-8 -*-
"""
Created on Tue May 20 01:55:55 2025

@author: Jyyyy
"""


from elasticsearch import Elasticsearch
import pandas as pd


es = Elasticsearch(
    hosts=["https://localhost:9200"],        
    basic_auth=("elastic", "elastic"),
    verify_certs=False,
    ssl_show_warn=False
)

# Use the scroll to get all matching documents in batches from Elasticsearch
def _scroll_all(index_name: str,
                query: dict,
                batch_size: int = 5_000,
                scroll: str = "2m") -> list[dict]:
    
    docs, resp = [], es.search(index=index_name, body=query,
                               size=batch_size, scroll=scroll)
    sid = resp["_scroll_id"]; docs.extend(resp["hits"]["hits"])
    while True:
        resp = es.scroll(scroll_id=sid, scroll=scroll)
        hits = resp["hits"]["hits"]
        if not hits:
            break
        docs.extend(hits)
    return [h["_source"] for h in docs]
    # Returns a list of _sources that satisfy query



def sentiment_diff(index_name: str, platform: str = "all"):
    """
    Calculate the difference in average sentiment scores between post and comment
    Return columns: post_id, sentiment_diff
    """
    # all posts
    must_post = [{"term": {"type": "post"}}]
    if platform != "all":
        must_post.append({"term": {"platform": platform}})
    q_post = {"_source": ["id", "sentiment_score"],
              "query": {"bool": {"must": must_post}}}

    post_df = (pd.DataFrame(_scroll_all(index_name, q_post))
               .rename(columns={"id": "post_id"})
               .set_index("post_id"))

    # all comments
    must_cmt = [{"term": {"type": "comment"}}]
    if platform != "all":
        must_cmt.append({"term": {"platform": platform}})
    q_cmt = {"_source": ["post_id", "sentiment_score"],
             "query": {"bool": {"must": must_cmt}}}

    comment_avg = (pd.DataFrame(_scroll_all(index_name, q_cmt))
                   .groupby("post_id")["sentiment_score"]
                   .mean()
                   .to_frame())

    # calculate diff 
    diff = (comment_avg["sentiment_score"] - post_df["sentiment_score"]) \
           .dropna().reset_index()
    diff.columns = ["post_id", "sentiment_diff"]
    return diff