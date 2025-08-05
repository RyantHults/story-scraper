import json
import sqlite3

def create_database_and_tables(cursor):
    """
    Creates the necessary tables using the provided database cursor.

    Args:
        cursor: A SQLite database cursor object.
    """
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            subreddit TEXT,
            scraped_at TEXT,
            total_posts INTEGER,
            total_user_comments INTEGER
        )
    ''')

    # Create posts table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS posts (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            title TEXT,
            text TEXT,
            score INTEGER,
            upvote_ratio REAL,
            num_comments INTEGER,
            created_utc REAL,
            created_datetime TEXT,
            url TEXT,
            permalink TEXT,
            user_comments_count INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id)
        )
    ''')

    # Create comments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            post_id TEXT,
            body TEXT,
            score INTEGER,
            created_utc REAL,
            created_datetime TEXT,
            permalink TEXT,
            parent_id TEXT,
            is_submitter BOOLEAN,
            depth INTEGER,
            FOREIGN KEY (post_id) REFERENCES posts (id)
        )
    ''')
    print("Database tables ensured to exist.")

def parse_and_insert_data(cursor, json_file):
    """
    Parses a JSON file and inserts the data using the provided database cursor.

    Args:
        cursor: A SQLite database cursor object.
        json_file (str): The path to the JSON file.
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except (IOError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing JSON file '{json_file}': {e}")
        return # Exit the function if the file can't be read

    # Insert user data, using INSERT OR IGNORE to prevent errors on re-runs
    cursor.execute('''
        INSERT OR IGNORE INTO users (username, subreddit, scraped_at, total_posts, total_user_comments)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        data['username'],
        data['subreddit'],
        data['scraped_at'],
        data['total_posts'],
        data['total_user_comments']
    ))
    
    # Get the user_id for the given username
    cursor.execute('SELECT id FROM users WHERE username = ?', (data['username'],))
    user_id_result = cursor.fetchone()
    if not user_id_result:
         print(f"Could not retrieve user ID for {data['username']}")
         return
    user_id = user_id_result[0]

    # Insert post data
    for post in data['posts']:
        cursor.execute('''
            INSERT OR REPLACE INTO posts (id, user_id, title, text, score, upvote_ratio, num_comments,
                             created_utc, created_datetime, url, permalink, user_comments_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            post['id'], user_id, post['title'], post['text'], post['score'],
            post['upvote_ratio'], post['num_comments'], post['created_utc'],
            post['created_datetime'], post['url'], post['permalink'], post['user_comments_count']
        ))

    # Insert comment data
    for post_id, comments_list in data['comments'].items():
        for comment in comments_list:
            cursor.execute('''
                INSERT OR REPLACE INTO comments (id, post_id, body, score, created_utc, created_datetime,
                                  permalink, parent_id, is_submitter, depth)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                comment['id'], comment['post_id'], comment['body'], comment['score'],
                comment['created_utc'], comment['created_datetime'], comment['permalink'],
                comment['parent_id'], comment['is_submitter'], comment['depth']
            ))
    print("Data inserted successfully.")


def main():
    """Main function to orchestrate database creation and data parsing."""
    json_filename = 'squigglestorystudios_hfy_activity.json'
    db_filename = 'reddit_activity.db'
    
    conn = None  # Initialize conn to None
    try:
        # The timeout parameter tells SQLite to wait for 5 seconds if the db is locked.
        conn = sqlite3.connect(db_filename, timeout=5.0) 
        cursor = conn.cursor()
        
        # Run all database operations within a single connection
        create_database_and_tables(cursor)
        parse_and_insert_data(cursor, json_filename)
        
        # Commit the transaction only after all operations succeed
        conn.commit()
        print("Transaction committed successfully.")

    except sqlite3.Error as e:
        print(f"A database error occurred: {e}")
        if conn:
            print("Rolling back transaction.")
            conn.rollback() # If anything fails, undo all changes from this run.
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


if __name__ == '__main__':
    # First, make sure any programs using the .db file are closed.
    # Then run the script.
    main()