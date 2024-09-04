import pymysql
from textblob import TextBlob
import logging
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import sys
import os
from datetime import datetime

# create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# rds settings
username = os.environ['USER_NAME']
password = os.environ['PASSWORD']
rds_proxy_host = os.environ['RDS_PROXY_HOST']
db_name = os.environ['DB_NAME']

# connect to rds
try:
    conn = pymysql.connect(host=rds_proxy_host,
                           user=username,
                           password=password,
                           db=db_name,
                           connect_timeout=5)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    logger.error(e)
    sys.exit(1)

# extract from rss feed
def extract(feed):
    response = Request(feed, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(urlopen(response), 'html.parser')
    headlines = []
    for item in soup.find_all('item'):
        title = item.title.text
        published = datetime.now().strftime('%Y-%m-%d')
        headlines.append({'Title': title, 'Published': published})
    return headlines

# helper for converting polarity to sentiment
def get_sentiment(polarity):
    if polarity > 0.33:
        return 'Positive'
    elif polarity < -0.33:
        return 'Negative'
    else:
        return 'Neutral'

# Get sentiment of headlines
def classify(headlines):
    logger.info(f"Number of headlines before classification: {len(headlines)}")
    for headline in headlines:
        headline['Sentiment'] = get_sentiment(TextBlob(headline['Title']).sentiment.polarity)
    return headlines

# load headlines into database
def load(headlines):
    logger.info(f"Number of headlines to load: {len(headlines)}")
    
    with conn.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS headlines (id SERIAL AUTO_INCREMENT PRIMARY KEY, Title VARCHAR(255), Published DATE, Sentiment VARCHAR(255))')
        for headline in headlines:
            cur.execute('INSERT INTO headlines (Title, Published, Sentiment) VALUES (%s, %s, %s)', 
                        (headline['Title'], headline['Published'], headline['Sentiment']))
    conn.commit()
    
    logger.info("Data loaded into database")

# handler for lambda function
def lambda_handler(event, context):
    feed = event.get('feed')
    if not feed:
        logger.error("No feed provided")
        return 'No feed provided'
    
    logger.info(f"Processing feed: {feed}")
    headlines = extract(feed)
    headlines = classify(headlines)
    load(headlines)
    return 'Success'