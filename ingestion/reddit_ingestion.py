import json
from datetime import datetime


def load_reddit_posts(path, limit=2000):
    """
    Load Reddit submissions (posts).
    Expected fields:
        - title
        - selftext
        - created_utc
    """

    posts = []

    if not path:
        return posts

    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):

                if i >= limit:
                    break

                try:
                    data = json.loads(line)

                    title = data.get("title", "")
                    body = data.get("selftext", "")
                    timestamp = data.get("created_utc")

                    if not timestamp:
                        continue

                    text = f"{title} {body}".strip()

                    if not text:
                        continue

                    posts.append({
                        "text": text,
                        "timestamp": datetime.utcfromtimestamp(timestamp)
                    })

                except Exception:
                    continue

    except FileNotFoundError:
        print("⚠️ Reddit posts file not found.")

    return posts


def load_reddit_comments(path, limit=3000):
    """
    Load Reddit comments.
    Expected fields:
        - body
        - created_utc
    """

    comments = []

    if not path:
        return comments

    try:
        with open(path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):

                if i >= limit:
                    break

                try:
                    data = json.loads(line)

                    text = data.get("body", "")
                    timestamp = data.get("created_utc")

                    if not text or not timestamp:
                        continue

                    comments.append({
                        "text": text.strip(),
                        "timestamp": datetime.utcfromtimestamp(timestamp)
                    })

                except Exception:
                    continue

    except FileNotFoundError:
        print("⚠️ Reddit comments file not found.")

    return comments