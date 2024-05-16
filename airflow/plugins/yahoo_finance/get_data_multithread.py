import yfinance as yf
from datetime import datetime, timedelta
import threading
from confluent_kafka import Producer
import json
import pandas as pd
from requests import Session

class YFinanceDataFetcher:
    def __init__(self, kafka_servers='broker:29092'):
        self.kafka_servers = kafka_servers
        self.kafka_topic = 'yfinance_data'
        self.producer_config = {
            'bootstrap.servers': self.kafka_servers,
            'client.id': 'yfinance-producer'
        }

    def daterange(self, start_date, end_date):
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        for n in range(int((end_date - start_date).days) + 1):
            start = start_date + timedelta(n)
            end = start + timedelta(1)
            yield (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    def get_historical_data(self, ticker, start_date, end_date, session):
        try:
            data = yf.download(tickers=ticker, start=start_date, end=end_date, auto_adjust=False, session=session)
            data.reset_index(inplace=True)
            if 'Date' in data.columns:
                data['Date'] = pd.to_datetime(data['Date'])
                data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
                json_data = {"Ticker": ticker, "Data": data.to_dict(orient='records')}
                return json_data
            else:
                raise ValueError("Date column not found in DataFrame")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    def send_data(self, ticker, start_date, end_date):
        session = Session()
        producer = Producer(**self.producer_config)
        try:
            for (d1,d2) in self.daterange(start_date, end_date):
                print(ticker, d1,d2)
                data = self.get_historical_data(ticker, d1, d2, session)
                if data:
                    def delivery_report(err, msg):
                        if err is not None:
                            print('Message delivery failed:', err)
                        else:
                            print('Message delivered to', msg.topic(), msg.partition())
                    producer.produce(self.kafka_topic, json.dumps(data).encode('utf-8'), callback=delivery_report)
                    producer.flush()
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
        finally:
            session.close()

    def fetch_and_send_data(self, tickers, start_date, end_date):
        threads = []
        for i in tickers:
            thread = threading.Thread(target=self.send_data, args=(i, start_date, end_date))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

    def run(self, tickers, start_date, end_date):
        self.fetch_and_send_data(tickers, start_date, end_date)