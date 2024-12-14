from neo4j import GraphDatabase
import csv

# Verbindung zur Neo4j-Datenbank herstellen
def create_neo4j_connection(uri, user, password):
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

# Funktion, um Autoren in Neo4j einzufügen
def create_author(driver, author_id):
    with driver.session() as session:
        session.run("MERGE (a:Author {id: $id})", id=author_id)

# Funktion, um Tweets in Neo4j einzufügen
def create_tweet(driver, tweet_id, text, created_at, retweet_count, like_count, fact_check):
    with driver.session() as session:
        session.run("""
        MERGE (t:Tweet {id: $id})
        SET t.text = $text, t.created_at = $created_at, 
            t.retweet_count = $retweet_count, t.like_count = $like_count, 
            t.fact_check = $fact_check
        """, 
        id=tweet_id, text=text, created_at=created_at, 
        retweet_count=retweet_count, like_count=like_count, fact_check=fact_check)

# Funktion, um Beziehungen in Neo4j einzufügen
def create_relationship(driver, author_id, tweet_id, relationship_type):
    with driver.session() as session:
        session.run("""
        MATCH (a:Author {id: $author_id}), (t:Tweet {id: $tweet_id})
        MERGE (a)-[r:%s]->(t)
        """ % relationship_type, author_id=author_id, tweet_id=tweet_id)

# Angepasste CSV-Dateien mit den Tweets, Autoren und Beziehungen
tweets_csv = r"C:\ZHAW_Repos\Twitter_Tweets\tweets1.csv"
authors_csv = r"C:\ZHAW_Repos\Twitter_Tweets\authors1.csv"
relationships_csv = r"C:\ZHAW_Repos\Twitter_Tweets\relationships1.csv"

# Verbindung zu Neo4j herstellen
driver = create_neo4j_connection("bolt://localhost:7687", "neo4j", "password")

# Autoren importieren
with open(authors_csv, 'r', newline='', encoding='utf-8') as authors_file:
    authors_reader = csv.reader(authors_file)
    next(authors_reader)  # Überspringe Header
    for row in authors_reader:
        author_id = row[0]
        create_author(driver, author_id)

# Tweets importieren
with open(tweets_csv, 'r', newline='', encoding='utf-8') as tweets_file:
    tweets_reader = csv.reader(tweets_file)
    next(tweets_reader)  # Überspringe Header
    for row in tweets_reader:
        tweet_id = row[0]
        text = row[1]
        created_at = row[2]
        retweet_count = int(row[3])
        like_count = int(row[4])
        fact_check = row[5] if row[5] else None
        create_tweet(driver, tweet_id, text, created_at, retweet_count, like_count, fact_check)

# Beziehungen importieren
with open(relationships_csv, 'r', newline='', encoding='utf-8') as relationships_file:
    relationships_reader = csv.reader(relationships_file)
    next(relationships_reader)  # Überspringe Header
    for row in relationships_reader:
        author_id = row[0]
        tweet_id = row[1]
        relationship_type = row[2].upper()  # Relationship type als Uppercase (z.B. POSTED, RETWEETED)
        create_relationship(driver, author_id, tweet_id, relationship_type)

# Verbindung schließen
driver.close()

print("Daten erfolgreich in Neo4j importiert.")
