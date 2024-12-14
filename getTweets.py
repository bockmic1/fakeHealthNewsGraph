import tweepy
import json

# Deine API-Schlüssel und Token
api_key = "4nk4Pfuhn1t1wTzismvsAhS4n"
api_key_secret = "XHpHQOeDkLVHSMMGBCjSDqbnFfpzDu5zXMGOMHThuyktifyYQw"
access_token = "1846536068331638784-e4hc9mYp1t0UHAECKU389KnHj6yVQm"
access_token_secret = "v2wy4Md8tquLKRbXqnCuMFSbwq6WHPLKxVTy84MUeHsUn"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAG%2FDwQEAAAAAKXOwbFo0kr3qIZmXJGs2AM9lTJo%3DTe7UOvrc5pVQf0AfLsY5Y3ZUjZ2WY1fKFvXyG5frgfmtUWxa2I"

# Authentifizierung mit Tweepy
client = tweepy.Client(bearer_token=bearer_token)

# Queries aufteilen
queries = [
    '"causes cancer" OR "cures cancer" OR "reduces cancer risk" OR "linked to cancer" OR "leads to cancer"',
    '"leads to diabetes" OR "reduces diabetes risk" OR "causes diabetes" OR "linked to diabetes" OR "prevents diabetes"',
    '"linked to heart disease" OR "reduces heart disease risk" OR "increases risk of heart attack" OR "causes heart disease"',
    '"causes autism" OR "vaccine effectiveness" OR "vaccine prevents disease" OR "linked to vaccine side effects"',
    '"reduces risk of infection" OR "causes obesity" OR "reduces obesity risk" OR "linked to weight gain" OR "leads to weight loss"',
    '"miracle cure" OR "natural remedies for cancer" OR "supplements for weight loss" OR "boosts immune system" OR "prevents diseases naturally"',
    '"studies show" OR "scientists say" OR "it is proven that" OR "new research shows" OR "reduces risk of illness"',
    '"COVID vaccine prevents" OR "reduces risk of COVID" OR "linked to COVID recovery" OR "prevents flu infection"',
    '"processed food risks" OR "reduces lung function" OR "linked to asthma" OR "air pollution causes"',
    '"leads to respiratory problems" OR "reduces chronic disease risk"'
]

def fetch_tweets(query, max_tweets=100):
    tweet_data = []
    next_token = None
    collected_tweets = 0

    while collected_tweets < max_tweets:
        try:
            response = client.search_recent_tweets(
                query=query + ' -is:retweet -politics -election -government has:links',
                max_results=min(100, max_tweets - collected_tweets),
                tweet_fields=["created_at", "author_id", "text", "public_metrics", "referenced_tweets"],
                user_fields=["id", "verified", "created_at", "public_metrics"],
                expansions=["author_id"],
                next_token=next_token
            )
            if response and response.data:
                users = {user["id"]: user for user in response.includes.get("users", [])}
                for tweet in response.data:
                    user_info = users.get(tweet.author_id, {})
                    is_retweet = False
                    original_tweet_id = None

                    if hasattr(tweet, "referenced_tweets") and tweet.referenced_tweets:
                        for ref_tweet in tweet.referenced_tweets:
                            if ref_tweet["type"] == "retweeted":
                                is_retweet = True
                                original_tweet_id = ref_tweet["id"]

                    user_created_at = user_info.get("created_at")
                    if user_created_at:
                        user_created_at = user_created_at.isoformat()

                    tweet_info = {
                        "id": tweet.id,
                        "text": tweet.text,
                        "author_id": tweet.author_id,
                        "created_at": tweet.created_at.isoformat(),
                        "retweet_count": tweet.public_metrics["retweet_count"],
                        "like_count": tweet.public_metrics["like_count"],
                        "user_verified": user_info.get("verified"),
                        "user_tweet_count": user_info.get("public_metrics", {}).get("tweet_count"),
                        "followers_count": user_info.get("public_metrics", {}).get("followers_count"),
                        "user_created_at": user_created_at,
                        "is_retweet": is_retweet,
                        "original_tweet_id": original_tweet_id
                    }

                    tweet_data.append(tweet_info)
                    collected_tweets += 1

                next_token = response.meta.get("next_token")
                if not next_token:
                    break
            else:
                break

            print(f"Abgerufen: {collected_tweets} Tweets für Query: {query}")

        except Exception as e:
            print(f"Fehler beim Abrufen der Tweets für Query '{query}': {e}")
            break

    return tweet_data

# Tweets abrufen und speichern
all_tweets = []
for query in queries:
    print(f"Starte Abruf für Query: {query}")
    all_tweets.extend(fetch_tweets(query, max_tweets=1000))  # Bis zu 1000 Tweets pro Query

file_path = "tweets_cancer.json"
with open(file_path, 'w', encoding='utf-8') as f:
    json.dump(all_tweets, f, ensure_ascii=False, indent=4)

print(f"Gesamtzahl der abgerufenen Tweets: {len(all_tweets)}")
