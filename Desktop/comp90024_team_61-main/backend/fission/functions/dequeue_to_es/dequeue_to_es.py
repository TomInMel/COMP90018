import logging
import json
from typing import Dict, List, Any
from flask import current_app, request
from elasticsearch8 import Elasticsearch


def main() -> str:

    # Initialize Elasticsearch client
    es_client: Elasticsearch = Elasticsearch(
        'https://elasticsearch-master.elastic.svc.cluster.local:9200',
        verify_certs=False,
        ssl_show_warn=False,
        basic_auth=('elastic', 'elastic')
    )

    # Validate and parse request payload
    request_data: List[Dict[str, Any]] = request.get_json(force=True)
    current_app.logger.info(f'Processing {len(request_data)} data')

    doc_type = request_data.get("type")
    if doc_type == "post":
        index_name = "reddit_posts"
    elif doc_type == "comment":
        index_name = "reddit_comments"
    else:
        index_name = "none_type"


    doc_id = f'{request_data["id"]}-{int(request_data["created_utc"])}'


    # index the document into Elasticsearch
    index_response = es_client.index(
        index=index_name,
        id=doc_id,
        body=request_data
    )

    current_app.logger.info(
        f'Indexed {doc_type or "unknown"} {doc_id} into "{index_name}" '
        f'- Version: {index_response["_version"]}'
    )


    return 'ok'
