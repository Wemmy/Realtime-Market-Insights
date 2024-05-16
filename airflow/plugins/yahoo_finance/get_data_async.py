
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf
import asyncio
from aiokafka import AIOKafkaProducer
import json
from datetime import datetime, timedelta
import pandas as pd

class YFinanceDataFetcher:

    def __init__(self, kafka_servers = 'broker:29092'):
        # Initializer might include more setup in a more complex class
        self.kafka_servers = kafka_servers
        self.kafka_topic = 'yfinance_data'
        self.producer = None

    async def start_kafka_producer(self):
        if not self.producer:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.kafka_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            await self.producer.start()

    async def stop_kafka_producer(self):
        if self.producer:
            await self.producer.stop()
            self.producer = None

    def get_historical_data(self, ticker, start_date, end_date):
        data = yf.download(ticker, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        # convert the date to date
        data['Date'] = pd.to_datetime(data['Date'])
        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
        json_data = {
            "Ticker": ticker,
            "Data": data.to_dict(orient='records')  # Serializing DataFrame to list of records
        }
        return json_data

    async def fetch_and_send_data(self, ticker, start_date, end_date):
        data = self.get_historical_data(ticker, start_date, end_date)
        await self.producer.send_and_wait(self.kafka_topic, value=data)

    def daterange(self, start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            start = start_date + timedelta(n)
            end = start + timedelta(1)
            yield (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    async def chain(self, tickers, start_date, end_date):
        await self.start_kafka_producer()

        try:
            # for testing: iterate over the timefram for each day
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            date_list = list(self.daterange(start_dt, end_dt))

            tasks = [self.fetch_and_send_data(ticker, d1, d2) for (d1,d2) in date_list for ticker in tickers]
            await asyncio.gather(*tasks)
        finally:
            await self.stop_kafka_producer()

    def fetch_and_send_data_asynchronously(self, tickers, start_date, end_date):
        asyncio.run(self.chain(tickers, start_date, end_date))
    
    # async def fetch_and_send_data_asynchronously(self, tickers, start_date, end_date):
    #     await self.start_kafka_producer()
    #     with ThreadPoolExecutor() as executor:
    #         future_to_ticker = {executor.submit(self.get_historical_data, ticker, start_date, end_date): ticker for ticker in tickers}

    #         for future in as_completed(future_to_ticker):
    #             ticker = future_to_ticker[future]
    #             try:
    #                 data = await asyncio.wrap_future(future)
    #                 await self.send_to_kafka(data,ticker)
    #             except Exception as e:
    #                 print(str(ticker)+ ':' +str(e))
    #     await self.stop_kafka_producer()
    #     return results
    
