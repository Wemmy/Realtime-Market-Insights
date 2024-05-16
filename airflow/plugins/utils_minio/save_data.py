from confluent_kafka import Consumer, KafkaError, KafkaException
import json
from minio import Minio
from minio.error import S3Error
from io import BytesIO
from datetime import datetime
import time
from alpha_vantage.get_data import NewsArticle
from yahoo_finance.get_data import News

class MinioDataConsumer:
    producer_config = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'consumer-group-1',
        'auto.offset.reset': 'earliest'
    }

    minio_config = {
        'endpoint': 'minio:9000',
        'access_key': 'admin',
        'secret_key': 'password',
        'secure': False
    }

    def __init__(self):
        self.consumer = Consumer(self.producer_config)
        self.minio_client = Minio(
            self.minio_config['endpoint'],
            access_key=self.minio_config['access_key'],
            secret_key=self.minio_config['secret_key'],
            secure=self.minio_config['secure']
        )
    
    def save_data(self, data, bucket_name, object_name):
        try:
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)

            # Convert data to a JSON string and encode to bytes
            data_bytes = json.dumps(data).encode('utf-8')
            data_length = len(data_bytes)
            # Create a BytesIO object
            data_stream = BytesIO(data_bytes)
            data_stream.seek(0)
            # Upload data
            self.minio_client.put_object(
                bucket_name,
                object_name,
                data=data_stream,
                length=data_length,
                content_type='application/json'
            )
            print(f"Saved batch to MinIO at {object_name}")
        except S3Error as exc:
            print("Error occurred while saving to MinIO: ", exc)

    def _gen_get_data(self, topic, mode='single', batch_size=100):
        '''
        batch is unfinished
        '''
        if mode =='batch':
            return self._consume_batch(batch_size)
        else:
            return self._consume_single()

    def _consume_batch(self, batch_size):
        # batch process
        batch_timeout = 10
        batch = []
        last_batch_time = time.time()

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                if time.time() - last_batch_time > batch_timeout:
                    if batch:
                        yield batch
                        batch = []
                        last_batch_time = time.time()
                    print("No new messages, ending consumption.")
                    break
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error():
                    # Handle other errors appropriately
                    print(f"Error: {msg.error()}")
                    continue

            batch.append(json.loads(msg.value().decode('utf-8')))
            if len(batch) >= batch_size:
                yield batch
                batch = []  # Reset the batch
                last_batch_time = time.time()

    def _consume_single(self):
        while True:
            msg = self.consumer.poll(timeout=1.0)

            if not msg:
                print("No new messages, ending consumption.")
                break

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print('%% %s [%d] reached end at offset %d\n' %(msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Handle other errors appropriately
                    raise KafkaException(msg.error())
            else:
                yield msg

    def consume_and_save_topic(self, topic, bucket_name = "raw"):
        self.consumer.subscribe([topic])
        for msg in self._gen_get_data(topic):
            headers = msg.headers()
            value = json.loads(msg.value().decode('utf-8'))
            source, ticker, date = self.extract_headers(headers)
            save_time = datetime.now().strftime("%H%M%S%f")
            object_name = f"{source}/{date}/{topic}/{save_time}.json"
            self.save_data(value, bucket_name, object_name)

    @staticmethod
    def extract_headers(headers):
        header_dict = {k: v.decode('utf-8') for k, v in headers}
        source = header_dict.get('source', None)
        ticker = header_dict.get('ticker', None)
        date = header_dict.get('end_date')
        if not date:
            date = header_dict.get('time_to',datetime.now().strftime("%Y-%m-%d"))
        return source, ticker, date

    def read_data(self, bucket_name, prefix):
        """Reads a JSON file from a MinIO bucket."""
        try:
            all_data = []
            objects = self.minio_client.list_objects(bucket_name,  prefix=prefix, recursive=True)
            for obj in objects:
                # Check if the object name matches the exact path or subpaths
                if obj.object_name.startswith(prefix):
                    response = self.minio_client.get_object(bucket_name, obj.object_name)
                    file_content = json.loads(response.read().decode('utf-8'))
                    all_data.append(file_content)
                    response.close()
            return all_data
        except S3Error as e:
            print(f"MinIO S3 error: {e}")
            return None

    def transform_alphavantage_news(self, input_bucket, output_bucket, output_key, prefix):
        """Reads a JSON file from MinIO, processes it using the NewsArticle class, and stores it back to MinIO."""
        data_list = self.read_data(input_bucket, prefix)
        if data_list:
            processed_data = []
            for data in data_list:
                if 'feed' in data:  # Assuming each file has a 'feed' key
                    articles = [NewsArticle(item) for item in data['feed']]
                    articles = [n.to_dict() for n in articles if n.is_text_valid()]
                    processed_data.extend(articles)
            object_name = f"{prefix.rstrip('/')}/{output_key}" if prefix else output_key
            self.save_data(processed_data, "transformed", object_name)
    
    def transform_yfinance_news(self, input_bucket, output_bucket, output_key, prefix):
        """Reads a JSON file from MinIO, processes it using the NewsArticle class, and stores it back to MinIO."""
        data_list = self.read_data(input_bucket, prefix)
        if data_list:
            processed_data = []
            for data in data_list:
                ticker = data['Ticker']
                news_list = data['Data']
                if news_list:  
                    articles = [News(item) for item in news_list]
                    articles = [n.to_dict() for n in articles if n.is_text_valid()]
                    processed_data.extend(articles)
            object_name = f"{prefix.rstrip('/')}/{output_key}" if prefix else output_key
            self.save_data(processed_data, "transformed", object_name)
