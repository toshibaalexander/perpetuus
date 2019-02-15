# Alpha Vantage adapter

import json
import urllib.request
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient

class AlphaVantageData:

    def __init__(self, url, symbol, startTime, endTime):
        self.url = url
        self.symbol = symbol
        self.startTime = startTime
        self.endTime = endTime
        self.data = []
    
    def parseData(self):
        with urllib.request.urlopen(self.url) as response:
            self.response = response
            raw = self.response.read()
            jsonObj = json.loads(raw.decode('utf-8'))
            for key in jsonObj['Time Series (Daily)'].keys():
                item = jsonObj['Time Series (Daily)'][key]
                document = {'Symbol': self.symbol,
                'Timestamp': str(key),
                'open': item['1. open'],
                'high': item['2. high'],
                'low': item['3. low'],
                'close': item['4. close'],
                'adjustedClose': item['5. adjusted close'],
                'volume': item['6. volume'],
                'dividendAmount': item['7. dividend amount'],
                'splitCoefficient': item['8. split coefficient']}
                self.data.append(document)
        return self.data

if __name__ == "__main__":
    client = MongoClient('localhost', 27017)
    databaseName = "perpetuus"
    collectionName = "AlphaVantage_StockTicks"
    db = client[databaseName]
    collection = db[collectionName]
    today = datetime.today().strftime("%Y-%m-%d")
    startDate = datetime.strptime("2019-02-14", "%Y-%m-%d")
    outputsize = np.busday_count( startDate, today )
    outputsizeStr = "compact"
    if outputsize > 100:
        outputsizeStr = "full"
    # replace with your alpha vantage api key here
    apikey = ""
    # list of stock symbols to look up e.g. GOOG
    symbols = []
    for symbol in symbols:
        # alpha vantage does not support querying between start and end dates so specify the earliest date that you want to retrieve data for
        # and query the returned results instead
        url = "https://www.alphavantage.co" + "/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=" + symbol + "&apikey=" + apikey + "&outputsize=" + outputsizeStr
        adapter = AlphaVantageData(url, symbol, startDate, today)
        data = adapter.parseData()
        collection.insert_many(data)