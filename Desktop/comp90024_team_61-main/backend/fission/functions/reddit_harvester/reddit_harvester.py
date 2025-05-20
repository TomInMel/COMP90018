import os
import praw
from prawcore.exceptions import PrawcoreException , ResponseException,TooManyRequests
import traceback
from typing import List, Dict, Tuple, Any
from datetime import datetime
import requests
import logging
import json
import redis
import time
import random

# Redis keys
STATUS_HASH = "subreddits:status"
PROCESSED_KEY_PREFIX = "processed:"
BATCH_SIZE = 100

# Fission queue endpoint
enqueue_url = "http://router.fission/enqueue/reddit"


def get_secret_value(key):
    path = os.path.join('/secrets/default/reddit-secrets', key)
    logging.info(f"Loading secret {key} from {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Secret file not found: {path}")
    with open(path) as f:
        return f.read().strip()


def get_config_value(key):
    path = os.path.join('/configs/default/reddit-params', key)
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return f.read().strip()


def create_reddit_client():
    logging.info("Initializing Reddit client...")
    client_id = get_config_value("CLIENT_ID")
    client_secret = get_secret_value("CLIENT_SECRET")
    username = get_config_value("REDDIT_USERNAME")
    password = get_secret_value("REDDIT_PASSWORD")
    user_agent = get_config_value("USER_AGENT")
    return praw.Reddit(
        client_id=client_id,
        client_secret=client_secret,
        username=username,
        password=password,
        user_agent=user_agent
    )

def get_subreddits(reddit: praw.Reddit, redis_client: redis.StrictRedis, subreddit_topic: str) -> list:

    all_subs = redis_client.hkeys(STATUS_HASH)
    
    if not all_subs:
        logging.info(f"No subreddits found in Redis. Initializing for topic: '{subreddit_topic}'")
        
        subreddits = []
        try:
            for result in reddit.subreddits.search(subreddit_topic, limit=None):
                subreddits.append(result.display_name)
            logging.info(f"Found {len(subreddits)} subreddits for topic: '{subreddit_topic}'")
        except PrawcoreException as e:
            logging.error(f"Error fetching subreddits: {e}")
            return []
        
        for sub in subreddits:
            redis_client.hset(STATUS_HASH, sub, "active")
            processed_key = f"processed:{sub}"
            redis_client.delete(processed_key)
        
        logging.info(f"Initialized {len(subreddits)} subreddits with status 'active'")
        all_subs = subreddits
    else:
        logging.info(f"Found {len(all_subs)} existing subreddits in Redis")
    
    return all_subs



def is_valid_comment(text: str) -> bool:
    return "I am a bot" not in text


def _send_payload(payload: dict, redis_client: redis.StrictRedis, subreddit_name: str):
    
    headers = {"Content-Type": "application/json"}
    try:
        resp = requests.post(enqueue_url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        logging.info(f"Enqueued {payload['type']} {payload['data'].get('id')} from r/{subreddit_name}")
    except requests.RequestException as e:
        logging.error(f"Failed to enqueue {payload['type']} {payload['data'].get('id')}: {e}")
        redis_client.hset(STATUS_HASH, subreddit_name, 'failed')



def load_all_comments_with_backoff(post, max_backoff=60):
    backoff = 1
    while True:
        try:
            post.comments.replace_more(limit=None)
            return post.comments.list()
        except TooManyRequests as e:
            logging.warning(f"Rate limited loading comments for {post.id}, sleeping {backoff}s...")
            if e.retry_after:
                sleep_time = float(e.retry_after)
                logging.info(f"Reddit suggests waiting {sleep_time} seconds")
            else:
                sleep_time = backoff
                backoff = min(backoff * 2, max_backoff)
            time.sleep(sleep_time)
            continue
        except Exception as e:
            logging.error(f"Unexpected error loading comments for {post.id}: {e}")
            raise

def get_search_results(reddit: praw.Reddit, redis_client: redis.StrictRedis, subreddit_name: str, query: str) -> list:
    search_results_key = f"search_results:{subreddit_name}"
    
    # check if search results are cached in Redis
    post_ids = redis_client.lrange(search_results_key, 0, -1)
    
    if not post_ids:
        logging.info(f"No cached search results for r/{subreddit_name}, performing search...")
        subreddit = reddit.subreddit(subreddit_name)
        post_ids = []
        
        for post in subreddit.search(query, limit=None, sort='comments', syntax='lucene'):
            post_ids.append(post.id)
            redis_client.rpush(search_results_key, post.id)
        
        # set expiration for search results
        # redis_client.expire(search_results_key, 60*60*24*7)
        logging.info(f"Cached {len(post_ids)} search results for r/{subreddit_name}")
    
    return post_ids




def fetch_and_enqueue(reddit: praw.Reddit, redis_client: redis.StrictRedis, subreddit_name: str, query: str, target_runtime=300) -> Tuple[bool, bool]:
    try:
        processed_key = PROCESSED_KEY_PREFIX + subreddit_name
        start_time = time.time()

        post_ids = get_search_results(reddit, redis_client, subreddit_name, query)


        cursor_key = f"cursor:{subreddit_name}"
        cursor = int(redis_client.get(cursor_key) or "0")
        processed_one = False


        while cursor < len(post_ids)  and time.time() - start_time < target_runtime:
            post_id = post_ids[cursor]
            
            # check if post is already processed
            if redis_client.sismember(processed_key, post_id):
                logging.info(f"Post {post_id} already processed in r/{subreddit_name}")
                cursor += 1
                continue

            # get post by ID
            logging.info(f"Fetching post {post_id} in r/{subreddit_name}")
            try:
                post = reddit.submission(id=post_id)
                logging.info(f"Processing post {post_id} ({cursor+1}/{len(post_ids)}) in r/{subreddit_name}")

                # get post comments
                logging.info(f"Fetching comments for post {post_id} in r/{subreddit_name}")
                try:
                    comments_forest = load_all_comments_with_backoff(post)
                    comments = [c for c in comments_forest if is_valid_comment(c.body)]
                except Exception as e:
                    logging.warning(f"Error loading comments for {post_id}: {e}")
                    comments = []

                # enqueue comments
                for comment in comments:
                    comment_payload = {
                        "type": "comment",
                        "query": query,
                        "subreddit": subreddit_name,
                        "data": {
                            "post_id": post_id,
                            "id": comment.id,
                            "body": comment.body,
                            "created_utc": comment.created_utc,
                            "author": str(comment.author),
                            "score": comment.score,
                            "ups": comment.ups,                                 
                            "downs": comment.downs, 
                        }
                    }
                    _send_payload(comment_payload, redis_client, subreddit_name)
                    processed_one = True

                comments_count = len(comments)

                # enqueue post data
                post_payload = {
                    "type": "post",
                    "query": query,
                    "subreddit": subreddit_name,
                    "data": {
                        "id": post_id,
                        "title": post.title,
                        "url": post.url,
                        "created_utc": post.created_utc,
                        "author": str(post.author),
                        "is_self": post.is_self,
                        "content": post.selftext,
                        "score": post.score,
                        "ups": post.ups,
                        "downs": post.downs,
                        "upvote_ratio": post.upvote_ratio,
                        "num_crossposts": post.num_crossposts,
                        "num_comments": comments_count
                    }
                }
                _send_payload(post_payload, redis_client, subreddit_name)
                processed_one = True
                redis_client.sadd(processed_key, post_id) 
            
            except Exception as e:
                logging.error(f"Error processing post {post_id}: {e}")
            
            # finish one post, update cursor
            cursor += 1
            redis_client.set(cursor_key, cursor)

        # update subreddit status in Redis
        sub_completed = cursor >= len(post_ids)
        if sub_completed:
            logging.info(f"Completed processing all posts for r/{subreddit_name}")
            redis_client.hset(STATUS_HASH, subreddit_name, 'finish')
            redis_client.delete(cursor_key)
        else:
            logging.info(f"Partially processed r/{subreddit_name}, cursor at {cursor}/{len(post_ids)}")
            redis_client.hset(STATUS_HASH, subreddit_name, 'processing')
        
        timed_out = time.time() - start_time >= target_runtime
        finished_without_timeout = sub_completed and not timed_out
        return processed_one, finished_without_timeout

    except PrawcoreException as e:
        logging.error(f"PRAW error in r/{subreddit_name}: {e}")
        redis_client.hset(STATUS_HASH, subreddit_name, 'failed')
        return False, False
    except Exception as e:
        logging.error(f"Unexpected error in r/{subreddit_name}: {e}")
        # may be small error, still mark it as processing first
        redis_client.hset(STATUS_HASH, subreddit_name, 'processing') 
        return False,False




def select_subreddit(reddit, redis_client,subreddit_topic :str, query: str) -> List[str]:

    logging.info("Selecting subreddit to process for this run...")
    
    # get all subreddits from Redis
    all_subs = get_subreddits(reddit, redis_client, subreddit_topic)
    if not all_subs:
        logging.info("No subreddits found in status hash.")
        return None
    
    # categorize subreddits based on their status
    processing_subs = []
    active_subs = []
    failed_subs = []
    finished_subs = []

    
    for sub in all_subs:
        status = redis_client.hget(STATUS_HASH, sub)
        if status == 'processing':
            processing_subs.append(sub)
        elif status == 'active':
            active_subs.append(sub)
        elif status == 'failed':  
            failed_subs.append(sub)
        elif status == 'finished':
            finished_subs.append(sub)
    
    # try to process 'processing' status subreddits first
    processed_subs: List[str] = []

    if processing_subs:
       
        random.shuffle(processing_subs)
        for sub in processing_subs:
            logging.info(f"Trying 'processing' subreddit: r/{sub}")
            processed_one, finished_without_timeout = fetch_and_enqueue(reddit, redis_client, sub, query)
            if not processed_one:
                logging.info(f"No work on r/{sub}, next processing sub…")
                continue

            logging.info(f"Processed r/{sub}")
            processed_subs.append(sub)

            if finished_without_timeout:
                logging.info(f"Finished r/{sub} without timeout, continue")
                continue

            logging.info(f"Timed out on r/{sub}, ending run.")
            return processed_subs
            

    
    # if no 'processing' subreddits, try 'active' ones
    if active_subs:
        random.shuffle(active_subs)
        for sub in active_subs:
            logging.info(f"Trying 'active' subreddit: r/{sub}")
            processed_one, finished_without_timeout = fetch_and_enqueue(reddit, redis_client, sub, query)
            if not processed_one:
                logging.info(f"No work on r/{sub}, next processing sub…")
                continue

            logging.info(f"Processed r/{sub}")
            processed_subs.append(sub)


            if finished_without_timeout:
                logging.info(f"Finished r/{sub} without timeout, continue")
                continue

            logging.info(f"Timed out on r/{sub}, ending run.")
            return processed_subs

    # if no 'processing' or 'active' subreddits, try 'failed' ones again
    if failed_subs:
        random.shuffle(failed_subs)
        for sub in failed_subs:
            logging.info(f"Trying 'failed' subreddit: r/{sub}")
            processed_one, finished_without_timeout = fetch_and_enqueue(reddit, redis_client, sub, query)
            if not processed_one:
                logging.info(f"No work on r/{sub}, next failed sub…")
                continue
            logging.info(f"Processed r/{sub}")
            processed_subs.append(sub)
            if finished_without_timeout:
                logging.info(f"Finished r/{sub} without timeout, continue")
                continue
            logging.info(f"Timed out on r/{sub}, ending run.")
            return processed_subs
        
    if len(finished_subs) == len(all_subs):
        logging.info("All subreddits finished. Nothing to do.")
        return []
    # if no subreddits were processed successfully
    logging.info("No successful processing in any subreddit.")
    return None


def cleanup_resources(redis_client=None):
    logging.info("Cleaning up resources...")
    
    if redis_client:
        try:
            redis_client.close()
            logging.info("Redis connection closed")
        except Exception as e:
            logging.warning(f"Error closing Redis connection: {e}")   
  

def main() -> str:
   
    logging.info("Starting Reddit Harvester...")
    start_time = time.time()

    query = (
    '("trump" OR "donald trump" OR "trump\'s" '
    'OR "Donald J. Trump" OR "DJT" OR "us president" '
    'OR "trump administration" OR "trump admin")'
    )

    subreddit_topic = "australia"
    

    reddit = None
    redis_client = None


    try:
        reddit = create_reddit_client()
        redis_client = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local',
            decode_responses=True,
            socket_connect_timeout=5  
        )
        processed_subs = select_subreddit(reddit, redis_client, subreddit_topic, query)
        if not processed_subs:
            logging.error("No active subreddit with new posts.")
            return json.dumps({
                "status": "Error",
                "message": "No active subreddit with new posts",
                "execution_time_seconds": round(time.time() - start_time, 2)
            })

    except Exception as e:
        logging.error(f"Error during execution: {e}")
        return json.dumps({
            "status": "Error",
            "message": f"Execution failed: {str(e)}",
        })
    finally:
        cleanup_resources(redis_client)



    
    end_time = time.time()
    execution_time = end_time - start_time

    logging.info(f"Total execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")
    return json.dumps({
        "status": "OK",
        "message": f"Successfully processed posts from {processed_subs}",
        "execution_time_seconds": round(execution_time, 2)
    })


