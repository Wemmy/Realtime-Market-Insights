import yfinance as yf
from datetime import datetime, timedelta
from confluent_kafka import Producer
import json
import pandas as pd
from requests import Session
from typing import List, Dict, Optional, Generator,Tuple

class YFinanceDataProducer:
    producer_config = {
        'bootstrap.servers': 'broker:29092',
        'client.id': 'yfinance-producer'
    }

    @staticmethod
    def daterange(start_date: str, end_date: str) -> Generator[Tuple[str, str], None, None]:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date = datetime.strptime(end_date, "%Y-%m-%d")
        for n in range(int((end_date - start_date).days) + 1):
            start = start_date + timedelta(n)
            end = start + timedelta(1)
            yield (start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    def __init__(self):
        self.producer = Producer(self.producer_config)

    def send_data(self, data, topic, headers):
        def delivery_report(err, msg):
            if err is not None:
                print('Message delivery failed:', err)
            else:
                print('Message delivered to', msg.topic(), msg.partition())
        self.producer.produce(topic, json.dumps(data).encode('utf-8'), headers=headers, callback=delivery_report)
        self.producer.flush()

    def get_historical_data(self, ticker, start_date, end_date, session) -> Optional[dict]:
        try:
            data = yf.download(tickers=ticker, start=start_date, end=end_date, auto_adjust=False, session=session)
            data.reset_index(inplace=True)
            if 'Date' in data.columns:
                data['Date'] = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')
                json_data = {"Ticker": ticker, "Data": data.to_dict(orient='records')}
                return json_data
            else:
                raise ValueError("Date column not found in DataFrame")
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return None

    def get_news_data(self, ticker, session) -> Optional[dict]:
        try:
            data = yf.Ticker(ticker, session=session).get_news()
            if data:
                json_data = {"Ticker": ticker, "Data": data}
                return json_data
            else:
                return None
        except Exception as e:
            print(f"Error fetching enws data for {ticker}: {e}")
            return None
    
    def get_and_send_historical_data(self, ticker, start_date, end_date):
        session = Session()
        try:
            for d1, d2 in self.daterange(start_date, end_date):
                print(ticker, d1, d2)
                data = self.get_historical_data(ticker, d1, d2,session)
                headers = [
                    ('ticker', ticker.encode('utf-8')), 
                    ('source', 'yfinance'.encode('utf-8')),
                    ('start_date', d1.encode('utf-8')),
                    ('end_date', d2.encode('utf-8'))
                    ]
                self.send_data(data, 'historical_data', headers)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
        finally:
            session.close()

    def get_and_send_news_data(self, ticker):
        session = Session()
        try:
            data = self.get_news_data(ticker, session)
            headers = [
                ('ticker', ticker.encode('utf-8')), 
                ('source', 'yfinance'.encode('utf-8')),
                ('date', datetime.now().strftime("%Y-%m-%d").encode('utf-8'))
                ]
            self.send_data(data, 'news_data', headers)
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
        finally:
            session.close()

from newsplease import NewsPlease

class News:

    def __init__(self, data):
        self.uuid = data['uuid']
        self.title = data['title']
        self.publisher = data['publisher']
        self.link = data['link']
        self.providerPublishTime = data['providerPublishTime']
        self.type = data['type']
        # self.thumbnail = data['thumbnail']
        self.relatedTickers = data['relatedTickers']
        self.main_text = self._extract_main_text(data['link'])

    @staticmethod
    def _extract_main_text(url):
        try:
            # Use news-please to extract main text from self.url
            article = NewsPlease.from_url(url)
            if article is not None and hasattr(article, 'maintext'):
                return article.maintext
        except HTTPError as e:
            print(f"HTTP error occurred: {e.code} - {e.reason}")
            return None
        except URLError as e:
            print(f"URL error occurred: {e.reason}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def is_text_valid(self):
        # Simple validation: Check if main_text is not None and has more than 100 words
        if self.main_text == "This page has not been authorized, sponsored, or otherwise approved or endorsed by the companies represented herein. Each of the company logos represented herein are trademarks of Microsoft Corporation; Dow Jones & Company; Nasdaq, Inc.; Forbes Media, LLC; Investor's Business Daily, Inc.; and Morningstar, Inc.\nCopyright 2024 Zacks Investment Research | 10 S Riverside Plaza Suite #1600 | Chicago, IL 60606\nAt the center of everything we do is a strong commitment to independent research and sharing its profitable discoveries with investors. This dedication to giving investors a trading advantage led to the creation of our proven Zacks Rank stock-rating system. Since 1988 it has more than doubled the S&P 500 with an average gain of +24.20% per year. These returns cover a period from January 1, 1988 through April 1, 2024. Zacks Rank stock-rating system returns are computed monthly based on the beginning of the month and end of the month Zacks Rank stock prices plus any dividends received during that particular month. A simple, equally-weighted average return of all Zacks Rank stocks is calculated to determine the monthly return. The monthly returns are then compounded to arrive at the annual return. Only Zacks Rank stocks included in Zacks hypothetical portfolios at the beginning of each month are included in the return calculations. Zacks Ranks stocks can, and often do, change throughout the month. Certain Zacks Rank stocks for which no month-end price was available, pricing information was not collected, or for certain other reasons have been excluded from these return calculations. Zacks may license the Zacks Mutual Fund rating provided herein to third parties, including but not limited to the issuer.\nVisit Performance Disclosure for information about the performance numbers displayed above.\nVisit www.zacksdata.com to get our data and content for your mobile app or website.\nReal time prices by BATS. Delayed quotes by Sungard.\nNYSE and AMEX data is at least 20 minutes delayed. NASDAQ data is at least 15 minutes delayed.\nThis site is protected by reCAPTCHA and the Google Privacy Policy and Terms of Service apply.":
            return False
        return self.main_text is not None

    def to_dict(self):
        # Convert the object properties including main text to dictionary
        return {
            "uuid": self.uuid,
            "title": self.title,
            "publisher": self.publisher,
            "link": self.link,
            "providerPublishTime": self.providerPublishTime,
            "type": self.type,
            "relatedTickers": self.relatedTickers,
            "main_text": self.main_text
        }
    