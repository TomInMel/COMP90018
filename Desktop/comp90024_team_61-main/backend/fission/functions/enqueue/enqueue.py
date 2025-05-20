import logging
import json
import gzip
from typing import Dict, Any, Optional
from flask import request, Request
import redis


def main() -> str:
    """Message queue producer for Redis streaming.

    Handles:
    - Redis connection pooling
    - JSON payload serialization
    - Topic-based message routing via headers
    - Message size logging
    - Both raw JSON and gzip compressed data

    Returns:
        'OK' with HTTP 200 on successful enqueue

    Raises:
        redis.RedisError: For connection/operation failures
        JSONDecodeError: If invalid payload received
    """
    req = request
    
    # Extract routing parameters
    topic: Optional[str] = req.headers.get('X-Fission-Params-Topic', 'reddit')  # 默认使用 'reddit' 主题
    
    content_encoding = req.headers.get('Content-Encoding', '').lower()
    
    try:
        if content_encoding == 'gzip':
            # if data if gzip compressed
            logging.info("Received gzip compressed data")
            raw_data = req.data
            decompressed_data = gzip.decompress(raw_data)
            json_data = json.loads(decompressed_data.decode('utf-8'))
            logging.info(f"Successfully decompressed data, size: {len(decompressed_data)} bytes")
        else:
            # if data is regular JSON
            logging.info("Received regular JSON data")
            json_data = req.get_json()
            
        # initialize Redis client    
        redis_client: redis.StrictRedis = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            socket_connect_timeout=5,
            decode_responses=False
        )
    
        payload_bytes = json.dumps(json_data).encode('utf-8')

        # push the payload to the Redis list
        redis_client.lpush(
            topic,
            payload_bytes
        )
        
        logging.info(
            f'============ Enqueue ============\n'
            f'Enqueued to {topic} topic - '
            f'Payload size: {len(payload_bytes)} bytes\n'
            f"================================="
        )
        
        return 'OK'
        
    except gzip.BadGzipFile:
        logging.error("Failed to decompress gzip data - invalid gzip format")
        return "Error: Invalid gzip data", 400
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON: {str(e)}")
        return f"Error: Invalid JSON - {str(e)}", 400
    except redis.RedisError as e:
        logging.error(f"Redis error: {str(e)}")
        return f"Error: Redis operation failed - {str(e)}", 500
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return f"Error: {str(e)}", 500