import pymysql
from textblob import TextBlob
import logging
import pandas as pd
import numpy as np
from xml.etree import ElementTree
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
import sys
import os
from datetime import datetime
import sqlalchemy


#create logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# rds settings


username = os.environ['USER_NAME']
password = os.environ['PASSWORD']
rds_proxy_host = os.environ['RDS_PROXY_HOST']
db_name = os.environ['DB_NAME']


#connect to rds
try:
    conn = pymysql.connect(host=rds_proxy_host,
                           user=username,
                           password=password,
                           db=db_name,
                           connect_timeout=5)
    engine = sqlalchemy.create_engine(f'mysql+pymysql://{username}:{password}@{rds_proxy_host}/{db_name}')
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySql instance.")
    logger.error(e)
    sys.exit(1)


#exrtract from rss feed
def extract(feed):
    response = Request(feed, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(urlopen(response), 'html.parser')
    headlines = pd.DataFrame(columns=['Title', 'Published'])
    for item in soup.find_all('item'):
        title = item.title.text
        published = datetime.now().strftime('%Y-%m-%d')
        headlines = pd.concat([headlines, pd.DataFrame({'Title': [title], 'Published': [published]})])
    return headlines
    

#helper for converting polarity to sentiment
def get_sentiment(polarity):
    if polarity > 0.33 :
        return 'Positive'
    elif polarity < -0.33:
        return 'Negative'
    else:
        return 'Neutral'
    
    
 
#Get sentiment of headlines   
def classify(headlines):
    logger.info(f"Number of headlines before classification: {len(headlines)}")
    headlines['Sentiment'] = headlines['Title'].apply(lambda x: get_sentiment(TextBlob(x).sentiment.polarity))
    return headlines
    

#create daily percent df
def get_daily_percent(headlines):
    headlines_dp = pd.get_dummies(headlines, columns=['Sentiment'], prefix='', prefix_sep='')
    headlines_dp.drop(columns=['Title'], inplace=True)
    headlines_dp = headlines_dp.groupby('Published').sum()
    headlines_dp['Total'] = headlines_dp.sum(axis=1)
    headlines_dp['Positive'] = headlines_dp['Positive'] / headlines_dp['Total']
    headlines_dp['Negative'] = headlines_dp['Negative'] / headlines_dp['Total']
    headlines_dp['Neutral'] = headlines_dp['Neutral'] / headlines_dp['Total']
    return headlines_dp
    


#load headlines into database
def load(headlines, headlines_dp):
    logger.info(f"Number of headlines to load: {len(headlines) + len(headlines_dp)}")
    
    with conn.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS headlines (id SERIAL AUTO_INCREMENT PRIMARY KEY, Title VARCHAR(255), Published DATE, Sentiment VARCHAR(255))')
        cur.execute('CREATE TABLE IF NOT EXISTS daily_percent (id SERIAL AUTO_INCREMENT PRIMARY KEY, Published DATE, Positive FLOAT, Negative FLOAT, Neutral FLOAT)')

    
    headlines.to_sql('headlines', engine, if_exists='append', index=False)
    headlines_dp.to_sql('daily_percent', engine, if_exists='append', index=False)
        
    logger.info("Data loaded into database")


#handler for lambda function
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

