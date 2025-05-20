import os
import uuid
from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)
# Elastic configuration
ES_HOST = "https://elasticsearch-master.elastic.svc:9200"
ES_USER = os.getenv("ES_USERNAME")
ES_PASS = os.getenv("ES_PASSWORD")

es = Elasticsearch(
    ES_HOST,
    basic_auth=(ES_USER, ES_PASS),
    headers={"Accept": "application/vnd.elasticsearch+json;compatible-with=8"},
    verify_certs=False
)

scroll_sessions = {}

# Fetching code
@app.route('/scroll_posts', methods=['GET','POST'])
def scroll_posts():
    try:
        token = request.args.get("token")
        scroll_time = request.args.get("scroll", "2m")
        size = int(request.args.get("size", 1000))

        if token:
            scroll_id = scroll_sessions.get(token)
            if not scroll_id:
                return jsonify({"error": "Invalid or expired token"}), 400

            result = es.scroll(scroll_id=scroll_id, scroll=scroll_time)
            scroll_id = result.get('_scroll_id')
            hits = result["hits"]["hits"]
        else:
            index = request.args.get("index")
            if not index:
                return jsonify({"error": "Missing 'index' parameter"}), 400

            if request.method == 'POST':
                query = request.get_json(force=True)
            else:
                query = {"query": {"match_all": {}}}

            result = es.search(index=index, body=query, scroll=scroll_time, size=size)
            scroll_id = result.get('_scroll_id')
            hits = result["hits"]["hits"]
            token = str(uuid.uuid4())

        if not hits:
            es.clear_scroll(scroll_id=scroll_id)
            scroll_sessions.pop(token, None)
            return jsonify({"token": None, "size": 0, "results": []})

        scroll_sessions[token] = scroll_id

        return jsonify({
            "token": token,
            "size": len(hits),
            "results": [hit["_source"] for hit in hits]
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
