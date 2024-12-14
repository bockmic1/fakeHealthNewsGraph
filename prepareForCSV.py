import json
import csv

# Pfad zur JSON-Datei
json_file = r"C:\ZHAW_Repos\Twitter_Tweets\tweets_cancer.json"

# Lade JSON-Daten
with open(json_file, 'r', encoding='utf-8') as file:
    tweets_data = json.load(file)

# CSV-Dateien erstellen
tweets_csv = r"C:\ZHAW_Repos\Twitter_Tweets\tweets1.csv"
authors_csv = r"C:\ZHAW_Repos\Twitter_Tweets\authors1.csv"
relationships_csv = r"C:\ZHAW_Repos\Twitter_Tweets\relationships1.csv"

# Öffne CSV-Dateien zum Schreiben
with open(tweets_csv, 'w', newline='', encoding='utf-8') as tweets_file1, \
     open(authors_csv, 'w', newline='', encoding='utf-8') as authors_file1, \
     open(relationships_csv, 'w', newline='', encoding='utf-8') as relationships_file1:
    
    tweets_writer = csv.writer(tweets_file1)
    authors_writer = csv.writer(authors_file1)
    relationships_writer = csv.writer(relationships_file1)
    
    # Header für die CSV-Dateien schreiben
    tweets_writer.writerow(['tweet_id', 'text', 'created_at', 'retweet_count', 'like_count', 'fact_check'])
    authors_writer.writerow(['author_id'])
    relationships_writer.writerow(['author_id', 'tweet_id', 'relationship_type'])
    
    # Set zum Speichern der Autoren (um Duplikate zu vermeiden)
    authors_set = set()
    
    # Iteriere über die Tweets und extrahiere die Informationen
    for tweet in tweets_data:
        tweet_id = tweet['id']
        author_id = tweet['author_id']
        text = tweet['text']
        created_at = tweet['created_at']
        retweet_count = tweet.get('retweet_count', 0)
        like_count = tweet.get('like_count', 0)
        fact_check = tweet.get('FactCheck', None)

        # Falls es sich um einen Retweet handelt, füge die Beziehung und den Original-Tweet hinzu
        if 'original_tweet' in tweet:
            original_tweet = tweet['original_tweet']
            original_tweet_id = original_tweet['id']
            original_author_id = original_tweet['author_id']
            
            # Speichere Original-Tweet-Daten
            tweets_writer.writerow([original_tweet_id, original_tweet['text'], original_tweet['created_at'], 
                                    original_tweet['retweet_count'], original_tweet['like_count'], original_tweet['FactCheck']])
            
            # Beziehung: Retweet
            relationships_writer.writerow([author_id, original_tweet_id, 'RETWEETED'])
            
            # Füge den Original-Autor zu den Autoren hinzu
            authors_set.add(original_author_id)
        else:
            # Speichere den normalen Tweet
            tweets_writer.writerow([tweet_id, text, created_at, retweet_count, like_count, fact_check])
            
            # Beziehung: Postet
            relationships_writer.writerow([author_id, tweet_id, 'POSTED'])
        
        # Füge den Autor zu den Autoren hinzu
        authors_set.add(author_id)

    # Speichere alle Autoren in der CSV-Datei
    for author_id in authors_set:
        authors_writer.writerow([author_id])

print("Daten wurden erfolgreich in CSV-Dateien gespeichert.")
