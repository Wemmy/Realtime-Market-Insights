import requests
from confluent_kafka import Producer
import json
from datetime import datetime,timedelta
from urllib.parse import urlencode
from urllib.error import URLError, HTTPError

class AlphaVantageAPIProducer:
    producer_config = {
        'bootstrap.servers': 'broker:29092',
        'client.id': 'alphavantage-producer'
    }

    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://www.alphavantage.co/query"
        self.producer = Producer(self.producer_config)

    def get_news_sentiment(self, tickers=None, topics=None, time_from=None, time_to=None, sort="LATEST", limit=50):
        """ Fetch news sentiment data from Alpha Vantage API based on provided criteria. """
        params = {
            "function": "NEWS_SENTIMENT",
            "apikey": self.api_key,
            "tickers": tickers,
            "topics": topics,
            "time_from": time_from,
            "time_to": time_to,
            "sort": sort,
            "limit": limit
        }
        url = self._construct_url(params)
        print(url)
        try:
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError for bad requests (4XX or 5XX)
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}

    def get_economic_indicator(self, f):
        params = {
            "function": f,
            "apikey": self.api_key
        }
        url = self._construct_url(params)
        print(url)
        try:
            response = requests.get(url)
            response.raise_for_status()  # Will raise an HTTPError for bad requests (4XX or 5XX)
            return response.json()
        except requests.RequestException as e:
            return {"error": str(e)}
    
    def _construct_url(self, params):
        processed_params = {}

        for key, value in params.items():
            if value is not None:
                if key in ['time_from', 'time_to']:
                    processed_params[key] = self._convert_date_to_api_format(value)
                else:
                    processed_params[key] = value

        query_string = urlencode(processed_params)
        if '?' in self.base_url:
            if self.base_url.endswith('?'):
                url = f"{self.base_url}{query_string}"
            else:
                url = f"{self.base_url}&{query_string}"
        else:
            url = f"{self.base_url}?{query_string}"
        return url


    def _convert_date_to_api_format(self, date_str):
        """ Convert a date string from 'yyyy-mm-dd' to 'YYYYMMDDTHHMM' format. """
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%Y%m%dT%H%M")

    def send_data(self, data, topic, headers):
        def acked(err, msg):
            if err is not None:
                print(f"Failed to deliver message: {err.str()}")
            else:
                print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        self.producer.produce(
            topic=topic, 
            value = json.dumps(data).encode('utf-8'), 
            headers=headers,
            callback=acked
            )
        self.producer.flush()

    def get_and_send_news(self, start_date=None, end_date=None):
        topics = ['blockchain', 'earnings', 'ipo', 'mergers_and_acquisitions', 'financial_markets', 'economy_fiscal', 'economy_monetary', 'economy_macro', 'energy_transportation','finance', 'life_sciences', 'manufacturing', 'real_estate', 'retail_wholesale','technology']

        if not (start_date and end_date):
            end_date = datetime.date()
            start_date = end_date - timedelta(1)
            end_date = end_date.strftime("%Y-%m-%d")
            start_date = start_date.strftime("%Y-%m-%d")

        for topic in topics:
            data = self.get_news_sentiment(tickers=None, topics=topic, time_from=start_date, time_to=end_date, sort='LATEST')
            headers = [
                ('source', 'alpha_vantage'.encode('utf-8')), 
                ('topic', topic.encode('utf-8')),
                ('time_from', start_date.encode('utf-8')), 
                ('time_to', end_date.encode('utf-8'))
                ]
            self.send_data(data, 'news', headers)

    def get_and_send_indicators(self):
        all_indicators = [
            'TREASURY_YIELD', 
            'FEDERAL_FUNDS_RATE', 
            'CPI', 
            'RETAIL_SALES',
            'DURABLES',
            'UNEMPLOYMENT',
            'NONFARM_PAYROLL'
            ]
        for f in all_indicators:
            data = self.get_economic_indicator(f)
            headers = [
                ('source', 'alpha_vantage'.encode('utf-8')), 
                ('indicator', f.encode('utf-8'))
                ]
            self.send_data(data, 'indicator', headers)

    # def get_active_list() -> list:
    #     # for testing only start with 50 tickers
    #     import random
    #     today = datetime.now().strftime('%Y-%m-%d')
    #     CSV_URL = f'https://www.alphavantage.co/query?function=LISTING_STATUS&date={today}&apikey={ALPHA_VANTAGE_API_KEY}'
    #     # Define the valid exchanges
    #     valid_exchanges = {'NYSE', 'TSX', 'NASDAQ'}
    #     with requests.Session() as s:
    #         download = s.get(CSV_URL)
    #         decoded_content = download.content.decode('utf-8')
    #         cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    #         active_tickers = {row[0] for row in cr if row and row[2] in valid_exchanges}
    #         return random.sample(list(active_tickers), 50)

    # def send_list_to_kafka_sync(tickers):

    #     async def send_list_to_kafka(tickers):
    #         """Sends the list of tickers to Kafka."""
    #         producer = AIOKafkaProducer(
    #             bootstrap_servers='broker:29092',
    #             value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    #         await producer.start()
    #         await producer.send_and_wait('ticker_list_topic', value={'tickers': tickers})
    #         await producer.stop()
    #     """Synchronous wrapper to send list to Kafka asynchronously."""
    #     asyncio.run(send_list_to_kafka(tickers))

    # def process_all_tickers(tickers):

    #     async def fetch_and_send_data(ticker):
    #         """Fetches stock data for a ticker and sends it to Kafka."""
    #         url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={ALPHA_VANTAGE_API_KEY}"
    #         async with aiohttp.ClientSession() as session:
    #             async with session.get(url) as response:
    #                 data = await response.json()
    #                 producer = AIOKafkaProducer(
    #                     bootstrap_servers='broker:29092',
    #                     value_serializer=lambda x: json.dumps(x).encode('utf-8'))
    #                 await producer.start()
    #                 await producer.send_and_wait('ticker_data_topic', value=data)
    #                 await producer.stop()

    #     async def main():
    #         await asyncio.gather(*(fetch_and_send_data(ticker) for ticker in tickers))

    #     """Processes all tickers asynchronously."""
    #     asyncio.run(main())
    

from newsplease import NewsPlease

class NewsArticle:
    def __init__(self, data):
        self.title = data['title']
        self.url = data['url']
        self.time_published = data['time_published']
        self.banner_image = data['banner_image']
        self.summary = data['summary']
        self.source = data['source']
        self.source_domain = data['source_domain']
        self.topics = data['topics']
        self.overall_sentiment_score = data['overall_sentiment_score']
        self.overall_sentiment_label = data['overall_sentiment_label']
        self.ticker_sentiment = data['ticker_sentiment']
        self.main_text = self._extract_main_text(data['url'])

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
            "title": self.title,
            "url": self.url,
            "time_published": self.time_published,
            "banner_image":self.banner_image,
            "summary": self.summary,
            "source": self.source,
            "source_domain": self.source_domain,
            "topics": self.topics,
            "overall_sentiment_score": self.overall_sentiment_score,
            "overall_sentiment_label": self.overall_sentiment_label,
            "ticker_sentiment": self.ticker_sentiment,
            "main_text": self.main_text
        }

# Function to process the JSON file
def process_json_file(filename):
    with open(filename, 'r') as file:
        data = json.load(file)
        articles = [NewsArticle(item) for item in data['feed']]
        return [a for a in articles if a.is_text_valid()]