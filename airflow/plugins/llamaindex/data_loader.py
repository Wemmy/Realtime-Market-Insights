import json
import pandas as pd
from utils_minio.save_data import MinioDataConsumer
from llama_index.core import Document
from minio import Minio
from datetime import datetime

class DataLoader:
    def __init__(self, input_bucket, prefix):
        # self.minio_config = minio_config
        self.input_bucket = input_bucket
        self.prefix = prefix

    def load_data_alpha_vantage(self):
        data_list = MinioDataConsumer().read_data(self.input_bucket, self.prefix)
        if data_list:
            # Create a list to store documents
            documents = []
            # Iterate througl all JSON in a folder
            for data in data_list:
                # Iterate through each entry in the JSON data
                for entry in data:
                    # Create a Document object
                    doc = Document(
                        id_=f"{entry['title']}{entry['time_published']}{entry['url']}{entry['source']}",
                        text=entry['main_text'],
                        metadata={
                            'title': entry['title'],
                            'time_published': int(datetime.strptime(entry['time_published'], '%Y%m%dT%H%M%S').timestamp()), #  convert '20240510T201000' to POSIX timestamp
                            'topics': sorted(entry['topics'], key=lambda x: float(x['relevance_score']), reverse=True)[0]['topic'],
                            'overall_sentiment_label': entry['overall_sentiment_label']
                        },
                        excluded_embed_metadata_keys=['overall_sentiment_label'],   # Ensure text is not indexed
                        excluded_llm_metadata_keys=['overall_sentiment_label'] # Ensure metadata is not indexed
                    )
                    # Append the Document to the list
                    documents.append(doc)
        return documents
    
    def load_data_yahoo_news(self):
        data_list = MinioDataConsumer().read_data(self.input_bucket, self.prefix)
        if data_list:
            documents = []
            for data in data_list:
                for entry in data:
                    # Create a Document object
                    doc = Document(
                        id_= entry['uuid'],
                        text=entry['main_text'],
                        metadata={
                            'title': entry['title'],
                            'time_published': datetime.utcfromtimestamp(entry['providerPublishTime']).strftime('%Y%m%dT%H%M%S'),
                            'relatedTickers': ','.join(entry['relatedTickers'])
                        }
                    )
                    documents.append(doc)
        return documents

    @staticmethod
    def docs_to_nodes(docs):
        from llama_index.core.node_parser import SentenceSplitter
        parser = SentenceSplitter(chunk_size=1024, chunk_overlap=20,)
        nodes = parser.get_nodes_from_documents(documents)
        return nodes



        