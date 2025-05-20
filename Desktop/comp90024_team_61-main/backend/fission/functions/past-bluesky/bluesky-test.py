import sys
import unittest
from bharvester import *


class bcharvesterUnitTest(unittest.TestCase):
    account = sys.argv[1]
    app_password = sys.argv[2]
    jwt = login_bluesky(account, app_password)
    
    # Test login function
    def test_login(self):
        self.assertNotEqual(self.jwt, "Login failed")

    # Test actor search function
    def test_search_actor(self):
        result = search_actor(self.jwt, 1)
        self.assertNotEqual(result, "Actor search failed")

    # Test profile get function
    def test_get_profile(self):
        # Get the did of a author
        time = '2025-05-11T00:00:00Z'
        post = search_posts(self.jwt, 1, "Trump",time)
        post_data = json.loads(post.text)

        # Check there is any post or not
        if len(post_data["posts"]) == 0:
            return "no posts"

        did = post_data["posts"][0]["author"]["did"]

        # Test get_profile function
        result = get_profile(self.jwt, did)
        self.assertNotEqual(result, "Profile get failed")

    # Test profiles get function
    def test_get_profiless(self):
        # Get the dids of a list of authors
        time = '2025-05-11T00:00:00Z'
        posts = search_posts(self.jwt, 10, "Trump",time)
        posts_data = json.loads(posts.text)
        
        # Check there is any post or not
        if len(posts_data["posts"]) == 0:
            return "no posts"
        
        did_list = [p['author']['did'] for p in posts_data["posts"]]
        profiles_data = get_profiles(self.jwt,did_list)

        # Test get_profiles function
        self.assertNotEqual(profiles_data, "Profiles get failed")

    # Test posts search function
    def test_search_posts(self):
        # Get the dids of a list of authors
        time = '2025-05-11T00:00:00Z'
        posts = search_posts(self.jwt, 10, "Trump",time)
        
        # Test search_posts function
        self.assertNotEqual(posts, "Posts search failed")

    # Test reply get function
    def test_get_reply(self):
        # Get the post 
        time = '2025-05-11T00:00:00Z'
        posts = search_posts(self.jwt, 1, "Trump",time)
        posts_data = json.loads(posts.text)
        
        # Get the reply of the post
        reply = get_reply(self.jwt,posts_data["posts"][0]["uri"])

        # Test get_reply function
        self.assertNotEqual(reply, "Reply get failed")

    # Test reply extract function
    def test_extract_reply(self):
        # Get the post 
        time = '2025-05-10T00:00:00Z'
        posts = search_posts(self.jwt, 1, "Trump",time)
        posts_data = json.loads(posts.text)
        
        # Get the reply of the post
        reply = json.loads(get_reply(self.jwt,posts_data["posts"][0]["uri"]).text)["thread"]
        
        replys = extract_reply(reply, posts_data["posts"][0]["cid"])

        # Test extract_reply function
        self.assertNotEqual(replys, "Reply extract failed")

    # Test all the function together
    def test_main(self):
        try:
            time = '2025-05-10T00:00:00Z'

            # Log in
            jwt = login_bluesky(self.account, self.app_password)
            self.assertNotEqual(jwt, "Login failed")

            # Search the post
            posts = search_posts(jwt, 100, "Trump",time)
            self.assertNotEqual(posts, "Posts search failed")

            posts_data = json.loads(posts.text)
            # Check there is any post or not
            if len(posts_data["posts"]) == 0:
                return
    

            # Send all the post to redis
            for post in posts_data["posts"]:

                reply = get_reply(jwt,post["uri"])
                self.assertNotEqual(reply, "Reply get failed")

                replys = json.loads(reply.text)["thread"]
                reply_list = extract_reply(replys, post["cid"])
                self.assertNotEqual(replys, "Reply extract failed")

        except Exception as e:
            self.fail("Fail: " + e)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'])