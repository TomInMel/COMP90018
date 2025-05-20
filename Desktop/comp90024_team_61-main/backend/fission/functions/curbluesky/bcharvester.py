import httpx
import json
import requests
import logging
import redis

# Login the bluesky and return the Jwt
def login_bluesky(handle: str, app_password: str):
    try:
        url = "https://bsky.social/xrpc/com.atproto.server.createSession"
        payload = {
            "identifier": handle,
            "password": app_password
        }
        response = httpx.post(url, json=payload)
        response.raise_for_status()
        return response.json()["accessJwt"]
    except Exception as e:
        return "Login failed"

# Search the actor
def search_actor(jwt_token, limit):
    try:
        url = "https://bsky.social/xrpc/app.bsky.actor.searchActors"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "q": "AU",
            "limit": limit,
        }
        response = httpx.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response
    except Exception as e:
        return "Actor search failed"
    
# Get the prfile of the actor according to his did
def get_profile(jwt_token, did):
    try:
        url = "https://bsky.social/xrpc/app.bsky.actor.getProfile"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "actor": did
        }
        response = httpx.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response
    except Exception as e:
        return "Profile get failed"
    
# Get a list of profiles according to a list of dids
def get_profiles(jwt_token, dids):
    try:
        url = "https://bsky.social/xrpc/app.bsky.actor.getProfiles"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "actors": dids
        }
        response = httpx.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response
    except Exception as e:
        return "Profiles get failed"
    

# Search post on bluesky according to the keyword
def search_posts(jwt_token, limit, keyword,time):
    try:
        url = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "q": keyword,
            "limit": limit,
            "lang": "en",
            "since": time
        }
        response = httpx.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response
    except Exception as e:
        return "Posts search failed"
    
# Get the reply according to the uri of the post
def get_reply(jwt_token, uri):
    try:
        url = "https://bsky.social/xrpc/app.bsky.feed.getPostThread"
        headers = {"Authorization": f"Bearer {jwt_token}"}
        params = {
            "uri": uri
        }
        response = httpx.get(url, headers=headers, params=params)
        
        response.raise_for_status()
        return response
    except Exception as e:
        return "Reply get failed"
    
# Get the config from shared-data
def config(k: str) -> str:
    try:
        with open(f'/configs/default/shared-data/{k}', 'r') as f:
            return f.read()
    except Exception as e:
        return "Config get failed"

# Extract all the reply from the result of get_reply function
def extract_reply(thread, post_id):
    try:
        replys = []
        if 'replies' in thread:
            for reply in thread['replies']:
                post = reply['post']
                post["post_id"] = post_id
                post["type"] = "comment"
                replys.append(post)
                replys.extend(extract_reply(reply,post_id))
        return replys
    except Exception as e:
        return "Reply extract failed"

def main():
    try:
        account = None
        app_password = None
        cursor = 0
        r = None
        jwt = None
        logging.info("I'm alive.")

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
            time = r.get("last_time")
            if time is None:
                time = '2025-05-11T00:00:00Z'

        except Exception as e:
            logging.error(f'Elasticsearch check failed: {str(e)}')
        
        # The key word
        australian_keywords = ["australia", "adelaide", "australian-born", "brisbane", 
        "canberra", "aussie", "sydney", "darwin", "melbourne", "perth",  "tasmania"]
        
        # Search the post
        posts = search_posts(jwt, 100, "Trump",time)
        posts_data = json.loads(posts.text)

        # Check there is any post or not
        if len(posts_data["posts"]) == 0:
            return "no posts"
        
        count = 0
        did_list = [p['author']['did'] for p in posts_data["posts"]]

        # Divide the posts into 4 groups, each group contain at most 25 posts
        did_25_list = [did_list[i:i+25] for i in range(0, len(did_list), 25)]

        for group in did_25_list:
            # Get the profiles of authors of above posts
            profiles_data = json.loads(get_profiles(jwt,group).text)["profiles"]
            for actor in profiles_data:
                # Check the profile contain any keyword in australian_keywords
                descri = actor.get("description", "").lower()
                if any(word in descri for word in australian_keywords):
                    posts_data["posts"][count]["type"] = "post"

                    response = requests.post(
                        url='http://router.fission/enqueue/current_bluesky',
                        headers={'Content-Type': 'application/json'},
                        json=posts_data["posts"][count]
                    )
                    logging.info("Sending post to enqueue:", posts_data["posts"][count])

                    # Get the comment of the post
                    replys = json.loads(get_reply(jwt,posts_data["posts"][count]["uri"]).text)["thread"]
                    reply_list = extract_reply(replys, posts_data["posts"][count]["cid"])

                    for reply in reply_list:
                        response = requests.post(
                            url='http://router.fission/enqueue/current_bluesky',
                            headers={'Content-Type': 'application/json'},
                            json=reply
                        ) 
                    
                count = count + 1

        time = posts_data["posts"][0]["record"]['createdAt']
        r.set("last_time", time)

        return 'OK from bluesky'
    except Exception as e:
        return "Fetch Bluesky failed"

