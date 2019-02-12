# Yahoo Finance adapter

import re
import json
import csv
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient
from yahoo_quote_download import yqd  # https://github.com/c0redumb/yahoo_quote_download

class YahooData_TimeSeriesDaily:

    def __init__(self, symbol, startTime, endTime):
        '''
        :param symbol: symbol code to extract from Yahoo (eg. CBA.AX)
        :param startTime: start date of extract window in format YYYY-MM-DD
        :param endTime: end date of extract window in format YYYY-MM-DD
        '''
        self.symbol = symbol
        self.startTime = startTime
        self.endTime = endTime
        self.data = []
        pass

    def parseData(self):
        # Convert dates to YYYYMMDD format
        date_start = self.startTime.replace('-','')
        date_end = self.endTime.replace('-','')

        # Extract data
        data_raw = yqd.load_yahoo_quote(self.symbol, date_start, date_end, info='quote')

        # Remove empty rows
        data_raw = [x for x in data_raw if len(x) > 0]

        # Split data by commas
        data_split = [x.split(',') for x in data_raw]
        data_item_length = [len(x) for x in data_split]
        if any(x != data_item_length[0] for x in data_item_length):
            raise ValueError("Inconsistent length of items")

        # Convert into dictionary
        data_dict = {}
        for i in range(data_item_length[0]):
            data_dict[data_split[0][i]] = [x[i] for x in data_split[1:]]  # first row are headers

        # Convert to pandas data frame
        data_pd = pd.DataFrame.from_dict(data_dict)

        # Convert data types
        data_pd['Date'] = pd.to_datetime(data_pd['Date'], format='%Y-%m-%d')
        non_date = data_pd.columns[data_pd.columns != 'Date'].get_values().tolist()
        data_pd[non_date] = data_pd[non_date].apply(pd.to_numeric, errors='coerce')

        # Remove null dates
        data_pd = data_pd[data_pd['Close'].notna()]

        # Remove monthly entries
        # Cut-off number of days between consecutive trading days before classifying as a monthly entry
        # should be large enough to take into account holidays and trading halts but less than one month
        diff_cutoff = 15
        data_pd.sort_values('Date', inplace=True)
        diff_date = data_pd['Date'].shift(-1) - data_pd['Date']
        data_pd = data_pd[diff_date.map(lambda x: x < pd.to_timedelta(diff_cutoff, unit='days'))]

        # Back out adjustments over time - set to NaN if date has no adjustments
        adj_ratio = data_pd['Adj Close'] / data_pd['Adj Close'].shift()
        close_ratio = data_pd['Close'] / data_pd['Close'].shift()
        round_cutoff = 4  # number of decimal places to consider to identify adjustments
        data_pd['Adjustments'] = adj_ratio / close_ratio
        data_pd['Adjustments'] = data_pd['Adjustments'].map(lambda x: np.nan if round(x, round_cutoff) == 1 else x)

        # Prepare data frame
        data_pd.rename(columns={'Date': 'Timestamp', 'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
                                'Volume': 'volume', 'Adjustments':'adjustment'},
                       inplace=True)
        data_pd.drop(['Adj Close'], axis=1, inplace=True)

        # Add symbol code
        data_pd['symbol'] = self.symbol

        # Convert to dictionary format for MongoDB
        self.data = []
        for index, row in data_pd.iterrows():
            document = row.to_dict()  # convert rows to dictionary
            if np.isnan(document['adjustment']): del document['adjustment']
            self.data.append(document)
        return self.data

if __name__ == "__main__":
    df = pd.read_csv('https://www.asx.com.au/asx/research/ASXListedCompanies.csv')
    client = MongoClient('localhost', 27017)
    databaseName = "perpetuus"
    collectionName = "ASX200_StockTicks"
    db = client[databaseName]
    collection = db[collectionName]
    today = datetime.today().strftime("%Y-%m-%d")
    startDate = datetime.today() - timedelta(days=12)
    startDate = startDate.strftime("%Y-%m-%d")
    for i in range(1, df.shape[0]):
        s = str(df.iloc[i,:]).split('\n')[1]
        symbol = s[s.find("(")+1:s.find(")")].split(',')[1].strip() + ".AX"
        adapter = YahooData_TimeSeriesDaily(symbol, startDate, today)
        data = adapter.parseData()
        collection.insert_many(data)