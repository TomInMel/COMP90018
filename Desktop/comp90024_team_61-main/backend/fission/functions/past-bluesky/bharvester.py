import httpx
import json
import requests
import logging
import redis

# Login the bluesky and return the Jwt
def login_bluesky(handle: str, app_password: str):
    url = "https://bsky.social/xrpc/com.atproto.server.createSession"
    payload = {
        "identifier": handle,
        "password": app_password
    }
    response = httpx.post(url, json=payload)
    response.raise_for_status()
    return response.json()["accessJwt"]

# Search the actor
def search_actor(jwt_token, limit):
    url = "https://bsky.social/xrpc/app.bsky.actor.searchActors"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {
        "q": "AU",
        "limit": limit,
    }
    response = httpx.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    return response

# Get the prfile of the actor according to his did
def get_profile(jwt_token, did):
    url = "https://bsky.social/xrpc/app.bsky.actor.getProfile"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {
        "actor": did
    }
    response = httpx.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    return response

# Get a list of profiles according to a list of dids
def get_profiles(jwt_token, dids):
    url = "https://bsky.social/xrpc/app.bsky.actor.getProfiles"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {
        "actors": dids
    }
    response = httpx.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    return response

# Search post on bluesky according to the keyword
def search_posts(jwt_token, limit, keyword,time):
    url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {
        "q": keyword,
        "limit": limit,
        "lang": "en",
        "until": time
    }
    response = httpx.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    return response

# Get the reply according to the uri of the post
def get_reply(jwt_token, uri):
    url = "https://bsky.social/xrpc/app.bsky.feed.getPostThread"
    headers = {"Authorization": f"Bearer {jwt_token}"}
    params = {
        "uri": uri
    }
    response = httpx.get(url, headers=headers, params=params)
    
    response.raise_for_status()
    return response
    
# Get the config from shared-data
def config(k: str) -> str:
    """Reads configuration from file."""
    with open(f'/configs/default/shared-data/{k}', 'r') as f:
        return f.read()

# Extract all the reply from the result of get_reply function
def extract_reply(thread, post_id):
    replys = []
    if 'replies' in thread:
        for reply in thread['replies']:
            post = reply['post']
            post["post_id"] = post_id
            post["type"] = "comment"
            replys.append(post)
            replys.extend(extract_reply(reply,post_id))
    return replys


def main():
    account = None
    app_password = None
    r = None
    jwt = None

    try:
        # Log in 
        account = config("HANDLE")
        app_password = config("APP_PASSWORD")
        jwt = login_bluesky(account, app_password)

        # Connect redis
        r = redis.StrictRedis(
            host='redis-headless.redis.svc.cluster.local', 
            port=6379,
            decode_responses=True
        )

        # Get the time 
        time = r.get("prev_time")
        if time is None:
            time = '2025-05-13T00:00:00Z'

    except Exception as e:
        logging.error(f'Elasticsearch check failed: {str(e)}')
    
    # Search the post
    posts = search_posts(jwt, 100, "Australia Trump",time)
    posts_data = json.loads(posts.text)

    # Send all the post to redis
    for post in posts_data["posts"]:
        response = requests.post(
            url='http://router.fission/enqueue/current_bluesky',
            headers={'Content-Type': 'application/json'},
            json=post
        )

        # Search and send all the comment of the post to the redis
        replys = json.loads(get_reply(jwt,post["uri"]).text)["thread"]
        reply_list = extract_reply(replys, post["cid"])

        for reply in reply_list:
            response = requests.post(
                url='http://router.fission/enqueue/current_bluesky',
                headers={'Content-Type': 'application/json'},
                json=reply
            ) 

    if posts_data["posts"]:
        time = posts_data["posts"][-1]["record"]['createdAt']
        r.set("prev_time", time)
    else:
        logging.warning("No new posts found.")
    r.set("prev_time", time)

    return 'OK from bluesky'

if __name__ == "__main__":
    posts = main()

