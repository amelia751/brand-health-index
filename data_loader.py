# pip install yfinance
import pandas as pd
from datetime import datetime
from datetime import timezone
import yfinance as yf

def read_complaint_csv(file_name):
    complaints = pd.read_csv(file_name)
    complaints.head()
    #complaints['Product'].value_counts().keys()
    complaints = complaints.rename(columns = {'Date received': 'Date', 'Consumer complaint narrative':'text', 'Company': 'bank'})
    complaints.columns = complaints.columns.str.lower()
    complaints["text"] = complaints["text"].replace("\n", "", regex=True).replace("\t", "", regex=True)
    complaints.columns
    complaints_df = complaints[['date', 'text', 'state', 'product', 'sub-product', 'issue', 'sub-issue','bank']]
    return complaints_df
    #dict(zip(complaints['Product'].value_counts().keys(), complaints['Product'].value_counts()))
    
#Tweets
def read_tweet_json(file_name):
    tweets = pd.read_json(file_name)
    #Year-Month-Day
    #T seperator
    #time: 
    #HH:mm:ss(Hour:Minute:Second) 24 hour
    #.000 seconds
    # Z is  UTC time zone
    tweets=tweets.rename(columns = {'ts_event': 'timestamp', 'brand_id': 'bank', 'author_location': 'location'})
    tweets['day_time'] = pd.to_datetime(tweets['timestamp']).dt.tz_convert('UTC')[0]
    tweets['date']=tweets['day_time'].dt.date
    tweets['time']=tweets['day_time'].dt.time
    #@Tony_BATtista @TDBank_US
    tweets['text'] = tweets['text'].replace("\n", "", regex=True).replace("\t", "", regex=True)
    tweets_df = tweets[["date", "time", "text", "location", "bank"]]
    #set(tweets_df["location"])
    return tweets_df   

def stock_data(stock_name):
    ticker = stock_name
    stock = yf.Ticker(ticker)
    stock = stock.history(period="6mo")
    stock = stock.reset_index()
    stock.columns = stock.columns.str.lower()
    stock
    stock['day_time'] =pd.to_datetime(stock['date'])
    stock['date']=stock['day_time'].dt.date
    stock['time']=stock['day_time'].dt.time
    #data = yf.download(ticker, start='2020-01-01', end='2021-01-01')
    #print(data.info())
    #print(data.head())
    #- timedelta(hours=4)
    stock_df = stock[['date', 'open', 'high', 'low', 'close', 'volume', 'time']]
    return stock_df


read_complaint_csv("complaints-2025-10-07_13_25.csv")
read_tweet_json("td_bank_tweets_master.json")
stock_data('TD')