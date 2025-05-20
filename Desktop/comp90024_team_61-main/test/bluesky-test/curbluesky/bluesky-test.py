import sys
import unittest
from bcharvester import *

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
            # The key word
            australian_keywords = ["australia", "adelaide", "australian-born", "brisbane", 
            "canberra", "aussie", "sydney", "darwin", "melbourne", "perth",  "tasmania"]
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
            
            count = 0
            did_list = [p['author']['did'] for p in posts_data["posts"]]

            # Divide the posts into 4 groups, each group contain at most 25 posts
            did_25_list = [did_list[i:i+25] for i in range(0, len(did_list), 25)]

            for group in did_25_list:
                # Get the profiles of authors of above posts
                profiles = get_profiles(jwt,group)
                self.assertNotEqual(profiles, "Profiles get failed")

                profiles_data = json.loads(profiles.text)["profiles"]
                for actor in profiles_data:
                    # Check the profile contain any keyword in australian_keywords
                    descri = actor.get("description", "").lower()
                    if any(word in descri for word in australian_keywords):
                        posts_data["posts"][count]["type"] = "post"

                        # Get the comment of the post
                        reply = get_reply(jwt,posts_data["posts"][count]["uri"])
                        self.assertNotEqual(reply, "Reply get failed")

                        replys = json.loads(reply.text)["thread"]
                        reply_list = extract_reply(replys, posts_data["posts"][count]["cid"])
                        self.assertNotEqual(replys, "Reply extract failed")

                    count = count + 1

        except Exception as e:
            self.fail("Fail: " + e)

if __name__ == '__main__':
    unittest.main(argv=['first-arg-is-ignored'])