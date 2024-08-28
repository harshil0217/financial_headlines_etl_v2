import pandas as pd
import numpy as np
import pymysql
import feedparser
import pendulum
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import os
import logging
import sys

feed = 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114'

#rds settings
username = os.environ['DB_USER']
password = os.environ['DB_PASS']
rds_proxy_host = os.environ['RDS_PROXY_HOST']
db_name = os.environ['DB_NAME']
    
logger = logging.getLogger() 
logger.setLevel(logging.INFO)
    
try:
    conn = pymysql.connect(rds_proxy_host, user=username, passwd=password, db=db_name, connect_timeout=5)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    logger.error(e)
    sys.exit(1)

def extract(feed):
    feed = feedparser.parse(feed)
    n = len(feed.entries)
    #create empty dataframe
    headlines = pd.DataFrame(np.nan, index = range(n), columns = ['Title', 'Link', 'Published'])
    #iteratively fill headlines dataframe
    for i in range(n):
        headlines['Title'][i] = feed.entries[i].title
        headlines['Link'][i] = feed.entries[i].link
        headlines['Published'] = feed.entries[i].published
    return headlines

def get_sentiment(x):
    if x['Sentiment'] > 0.05:
        return 'Positive'
    elif x['Sentiment'] < -0.05:
        return 'Negative'
    else:
        return 'Neutral'
    


def classify(headlines):
    sia = SentimentIntensityAnalyzer()
    headlines['Sentiment'] = headlines['Title'].apply(lambda x: sia.polarity_scores(x)['compound'])
    headlines['Sentiment'] = headlines.apply(lambda x: get_sentiment(x), axis = 1)
    return headlines


def load(headlines):
    # export dataframe to sql
    headlines.to_sql('headlines', conn, if_exists='append', index = False)
     
    
    
    
    