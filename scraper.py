#!/usr/bin/env python3
"""
Reddit User Posts and Comments Scraper

This script fetches all text posts made by a specific user in a given subreddit,
along with all comments that user made on those posts.

Requirements:
- praw library: pip install praw
- Reddit API credentials (client_id, client_secret, user_agent)

Setup:
1. Go to https://www.reddit.com/prefs/apps
2. Create a new app (script type)
3. Note your client_id and client_secret
4. Replace the credentials in the script below
"""

import json
import os
import praw
import time
from datetime import datetime
from dotenv import load_dotenv
from typing import List, Dict, Any

load_dotenv() 

class RedditUserScraper:
    def __init__(self, client_id: str, client_secret: str, user_agent: str):
        """
        Initialize the Reddit scraper with API credentials.
        
        Args:
            client_id: Reddit app client ID
            client_secret: Reddit app client secret
            user_agent: User agent string (e.g., "script:appname:v1.0 (by u/yourusername)")
        """
        self.reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent
        )
    
    def get_user_posts_in_subreddit(self, username: str, subreddit_name: str, limit: int = None) -> List[Dict[str, Any]]:
        """
        Get all posts made by a user in a specific subreddit.
        
        Args:
            username: Reddit username (without u/ prefix)
            subreddit_name: Subreddit name (without r/ prefix)
            limit: Maximum number of posts to fetch (None for all available)
            
        Returns:
            List of dictionaries containing post data
        """
        try:
            user = self.reddit.redditor(username)
            subreddit = self.reddit.subreddit(subreddit_name)
            
            posts = []
            
            # Get user's submissions and filter by subreddit
            for submission in user.submissions.new(limit=limit):
                if submission.subreddit.display_name.lower() == subreddit_name.lower():
                    # Only include text posts (self posts)
                    if submission.is_self and submission.selftext:
                        post_data = {
                            'id': submission.id,
                            'title': submission.title,
                            'text': submission.selftext,
                            'score': submission.score,
                            'upvote_ratio': submission.upvote_ratio,
                            'num_comments': submission.num_comments,
                            'created_utc': submission.created_utc,
                            'created_datetime': datetime.fromtimestamp(submission.created_utc).isoformat(),
                            'url': submission.url,
                            'permalink': f"https://reddit.com{submission.permalink}"
                        }
                        posts.append(post_data)
            
            return posts
            
        except Exception as e:
            print(f"Error fetching posts: {e}")
            return []
    
    def get_user_comments_on_posts(self, username: str, post_ids: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get comments made by a user on specific posts, but only:
        1. Top-level comments by the user
        2. Comments that are part of reply chains stemming from the user's top-level comments
        
        Args:
            username: Reddit username
            post_ids: List of post IDs to check for user comments
            
        Returns:
            Dictionary mapping post IDs to lists of comment data
        """
        try:
            user = self.reddit.redditor(username)
            comments_by_post = {}
            
            # First pass: collect all user comments and identify top-level ones
            all_user_comments = {}  # comment_id -> comment_data
            top_level_comment_ids = set()
            
            for comment in user.comments.new(limit=None):
                post_id = comment.submission.id
                
                # Check if this comment is on one of our target posts
                if post_id in post_ids:
                    comment_data = {
                        'id': comment.id,
                        'body': comment.body,
                        'score': comment.score,
                        'created_utc': comment.created_utc,
                        'created_datetime': datetime.fromtimestamp(comment.created_utc).isoformat(),
                        'permalink': f"https://reddit.com{comment.permalink}",
                        'parent_id': comment.parent_id,
                        'is_submitter': comment.is_submitter,
                        'post_id': post_id,
                        'depth': 0  # Will be calculated later
                    }
                    
                    all_user_comments[comment.id] = comment_data
                    
                    # Check if this is a top-level comment (parent is the submission)
                    if comment.parent_id.startswith('t3_'):  # t3_ prefix means it's replying to a submission
                        top_level_comment_ids.add(comment.id)
                        comment_data['depth'] = 0
            
            # Second pass: trace reply chains from top-level comments
            def is_in_user_chain(comment_id: str, visited: set = None) -> bool:
                """
                Check if a comment is part of a chain that stems from a user's top-level comment.
                """
                if visited is None:
                    visited = set()
                
                if comment_id in visited:
                    return False
                visited.add(comment_id)
                
                if comment_id in top_level_comment_ids:
                    return True
                
                comment_data = all_user_comments.get(comment_id)
                if not comment_data:
                    return False
                
                parent_id = comment_data['parent_id']
                
                # If parent is submission, this is top-level but not by our user
                if parent_id.startswith('t3_'):
                    return False
                
                # Extract comment ID from parent_id (format: t1_commentid)
                parent_comment_id = parent_id[3:] if parent_id.startswith('t1_') else parent_id
                
                # Check if parent is also a user comment in our chain
                return parent_comment_id in all_user_comments and is_in_user_chain(parent_comment_id, visited)
            
            # Calculate depths and filter comments
            for comment_id, comment_data in all_user_comments.items():
                if comment_id in top_level_comment_ids or is_in_user_chain(comment_id):
                    post_id = comment_data['post_id']
                    
                    # Calculate depth for non-top-level comments
                    if comment_id not in top_level_comment_ids:
                        depth = 0
                        current_parent = comment_data['parent_id']
                        
                        while current_parent.startswith('t1_'):
                            depth += 1
                            parent_comment_id = current_parent[3:]
                            if parent_comment_id in all_user_comments:
                                current_parent = all_user_comments[parent_comment_id]['parent_id']
                            else:
                                break
                        
                        comment_data['depth'] = depth
                    
                    if post_id not in comments_by_post:
                        comments_by_post[post_id] = []
                    
                    comments_by_post[post_id].append(comment_data)
            
            # Sort comments by creation time within each post
            for post_id in comments_by_post:
                comments_by_post[post_id].sort(key=lambda x: x['created_utc'])
            
            return comments_by_post
            
        except Exception as e:
            print(f"Error fetching comments: {e}")
            return {}
    
    def scrape_user_activity(self, username: str, subreddit_name: str, limit: int = None) -> Dict[str, Any]:
        """
        Complete scraping function that gets posts and comments.
        
        Args:
            username: Reddit username
            subreddit_name: Subreddit name
            limit: Maximum number of posts to fetch
            
        Returns:
            Dictionary containing all scraped data
        """
        print(f"Scraping posts by u/{username} in r/{subreddit_name}...")
        
        # Get user's posts in the subreddit
        posts = self.get_user_posts_in_subreddit(username, subreddit_name, limit)
        
        if not posts:
            print("No text posts found.")
            return {'posts': [], 'comments': {}}
        
        print(f"Found {len(posts)} text posts.")
        
        # Get post IDs
        post_ids = [post['id'] for post in posts]
        
        print("Fetching user comments on these posts...")
        
        # Get user's comments on these posts
        comments = self.get_user_comments_on_posts(username, post_ids)
        
        # Add comment counts to posts
        for post in posts:
            post['user_comments_count'] = len(comments.get(post['id'], []))
        
        result = {
            'username': username,
            'subreddit': subreddit_name,
            'scraped_at': datetime.now().isoformat(),
            'total_posts': len(posts),
            'total_user_comments': sum(len(comments_list) for comments_list in comments.values()),
            'posts': posts,
            'comments': comments
        }
        
        return result

def main():
    # Reddit API credentials - REPLACE THESE WITH YOUR OWN

    CLIENT_ID = os.getenv("CLIENT_ID") or ""
    CLIENT_SECRET = os.getenv("CLIENT_SECRET") or ""
    USER_AGENT = os.getenv("USER_AGENT")
    
    # Configuration
    USERNAME = "squigglestorystudios"  # Username to scrape (without u/ prefix)
    SUBREDDIT = "hfy"  # Subreddit to search in (without r/ prefix)
    LIMIT = 100  # Maximum posts to check (None for all available)
    
    # Validate credentials
    if CLIENT_ID == "" or CLIENT_SECRET == "":
        print("Error: Please replace CLIENT_ID and CLIENT_SECRET with your actual Reddit API credentials.")
        print("Get them from: https://www.reddit.com/prefs/apps")
        return
    
    try:
        # Initialize scraper
        scraper = RedditUserScraper(CLIENT_ID, CLIENT_SECRET, USER_AGENT)
        
        # Scrape data
        data = scraper.scrape_user_activity(USERNAME, SUBREDDIT, LIMIT)
        
        # Save to JSON file
        filename = f"{USERNAME}_{SUBREDDIT}_activity.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Print summary
        print(f"\nScraping complete!")
        print(f"Posts found: {data['total_posts']}")
        print(f"User comments on posts: {data['total_user_comments']}")
        print(f"Data saved to: {filename}")
        
        # Print first post as example
        if data['posts']:
            print(f"\nExample post:")
            post = data['posts'][0]
            print(f"Title: {post['title']}")
            print(f"Text: {post['text'][:200]}...")
            print(f"User comments on this post: {post['user_comments_count']}")
            
            # Show comment chain structure if comments exist
            post_id = post['id']
            if post_id in data['comments'] and data['comments'][post_id]:
                print(f"\nComment chain structure:")
                for comment in data['comments'][post_id]:
                    indent = "  " * comment['depth']
                    comment_preview = comment['body'][:100].replace('\n', ' ')
                    print(f"{indent}└─ Depth {comment['depth']}: {comment_preview}...")
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()