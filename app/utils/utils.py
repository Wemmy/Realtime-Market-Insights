
import streamlit as st
from minio import Minio
from datetime import datetime
import json
import re
from dotenv import load_dotenv
load_dotenv()
import os

minio_config = {
    'endpoint': 'localhost:9000',
    'access_key': 'admin',
    'secret_key': 'password',
    'secure': False
}

@st.cache_resource
def read_data(bucket_name, prefix):
    minio_client =  Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )
    all_data = []
    objects = minio_client.list_objects(bucket_name, prefix=prefix, recursive=True)
    for obj in objects:
        # Check if the object name matches the exact path or subpaths
        if obj.object_name.startswith(prefix):
            response = minio_client.get_object(bucket_name, obj.object_name)
            file_content = json.loads(response.read().decode('utf-8'))
            all_data.append(file_content)
            response.close()
    return all_data


@st.cache_resource
def get_alhpa_vantage_news(search_date = '20240510', search_topic=None, search_sentiment= None):
    # get all news
    prefix = 'alpha_vantage/2024-05-11/news/'
    data_list = read_data('transformed', prefix)

    filtered = []
    for data in data_list:
        for record in data:
            # Initialize match flags for each criterion
            date_match = False
            topic_match = False
            sentiment_match = False

            # Date filter (only if search_date is not None)
            if search_date:
                search_date_dt = datetime.strptime(search_date, "%Y%m%d").date()
                record_date_dt = datetime.strptime(record['time_published'][:8], "%Y%m%d").date()
                date_match = (record_date_dt == search_date_dt)
            else:
                date_match = True  # No date filter
            
            # Topic filter (only if search_topic is not None)
            if search_topic:
                for topic_dict in record.get('topics', []):
                    if topic_dict['topic'].lower() == search_topic.lower():
                        topic_match = True
                        break
            else:
                topic_match = True  # No topic filter
            
            # Sentiment filter (only if search_sentiment is not None)
            if search_sentiment:
                sentiment_match = (record.get('overall_sentiment_label') == search_sentiment)
            else:
                sentiment_match = True  # No sentiment filter

            # Add record if it matches all active filters
            if date_match and topic_match and sentiment_match:
                filtered.append(record)
    return filtered

# Function to determine color based on sentiment score
def get_color(sentiment_score):
    if sentiment_score < 0:
        # Closer to -1, more red
        red = 255
        green = int(255 * (1 + sentiment_score))  # Increase green as score moves to 0
    else:
        # Closer to 1, more green
        green = 255
        red = int(255 * (1 - sentiment_score))  # Decrease red as score moves to 1
    return f'rgb({red}, {green}, 0)'


def show_news(list_news, number_of_news_per_column = 2):
    i = 0
    while True:
        if i>number_of_news_per_column:
            break
        data= list_news[i]
        if not data['banner_image']:
            continue
        st.image(data['banner_image'])  # Display image
        st.markdown(
            f"<h4 style='font-size: medium;'><a href='{data['url']}'>{data['title']}</a></h4>", unsafe_allow_html=True
        )  # Display title as hyperlink
        # Determine color based on sentiment score
        color = get_color(data["overall_sentiment_score"])
        # set a tag
        st.markdown(
            f"<span style='display: inline-block; border: 1px solid #ddd; padding: 5px 10px; border-radius: 5px; background-color: {color};'>{data['overall_sentiment_label']}</span>",
            unsafe_allow_html=True
        )
        st.markdown(re.sub(r'(?<!\$)\$(?!\$)', r'\$', data['summary']))  # Display HTML content
        i+=1


def fetch_generated_content(collection, filter_dict, prompt):
    import chromadb
    import openai
    from llama_index.core import VectorStoreIndex,get_response_synthesizer
    from llama_index.core.vector_stores import MetadataFilter,MetadataFilters,FilterOperator,FilterCondition
    from llama_index.vector_stores.chroma import ChromaVectorStore
    from IPython.display import Markdown, display
    from llama_index.core.retrievers import VectorIndexRetriever
    from llama_index.core.query_engine import RetrieverQueryEngine
    
    openai.api_key = os.environ.get('OPENAI_API_KEY')
    chroma_client = chromadb.HttpClient(host='localhost', port=8000)
    chroma_collection = chroma_client.get_collection(name=collection)
    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    index = VectorStoreIndex.from_vector_store(vector_store)

    # dynamically build a filter
    # filters_time, filters_sentiment, filter_topic = [],[],[]
    f = []
    for key, value in filter_dict.items():
        # TO DO: Llamaindex not support multiple Metadatafilters
        if key == 'time_published':
            f.append(MetadataFilter(key=key, operator=FilterOperator.GTE, value=value))
            f.append(MetadataFilter(key=key, operator=FilterOperator.LT, value=value+86400))
        if key == 'overall_sentiment_label':
            f.append(MetadataFilter(key=key, operator=FilterOperator.EQ, value=value))
        if key == 'topics':
            f.append(MetadataFilter(key=key, operator=FilterOperator.EQ, value=value))

    filters = MetadataFilters(filters=f, condition=FilterCondition.AND)

    # configure retriever
    retriever = VectorIndexRetriever(
        index=index,
        filters=filters,
        similarity_top_k = 100
    )

    # configure response synthesizer
    response_synthesizer = get_response_synthesizer()

    # assemble query engine
    query_engine = RetrieverQueryEngine(
        retriever=retriever,
        response_synthesizer=response_synthesizer
    )
    
    response = query_engine.query(prompt)
    return re.sub(r'(?<!\$)\$(?!\$)', r'\$', response.response)


@st.cache_resource
def query_data(indicator):
    from minio import Minio
    import pandas as pd
    from io import BytesIO
    bucket_name = 'transformed'
    minio_config = {
        'endpoint': 'localhost:9000',
        'access_key': 'admin',
        'secret_key': 'password',
        'secure': False
    }
    minio_client =  Minio(
        minio_config['endpoint'],
        access_key=minio_config['access_key'],
        secret_key=minio_config['secret_key'],
        secure=minio_config['secure']
    )
    prefixs = {
        'TREASURY_YIELD': 'alpha_vantage/indicator/name=10-Year Treasury Constant Maturity Rate',
        'RETAIL_SALES': 'alpha_vantage/indicator/name=Advance Retail Sales%3A Retail Trade',
        'FEDERAL_FUNDS_RATE':'alpha_vantage/indicator/name=Effective Federal Funds Rate', 
        'CPI':'alpha_vantage/indicator/name=Consumer Price Index for all Urban Consumers', 
        'DURABLES':'alpha_vantage/indicator/name=Manufacturer New Orders%3A Durable Goods', 
        'UNEMPLOYMENT':'alpha_vantage/indicator/name=Unemployment Rate',
        'NONFARM-PAYROLL':'alpha_vantage/indicator/name=Total Nonfarm Payroll'
    }
    # List objects in the bucket with the specified prefix
    objects = minio_client.list_objects(bucket_name, prefix=prefixs.get(indicator), recursive=True)
    print(objects)
    # Initialize an empty list to hold DataFrames
    dataframes = []

    for obj in objects:
        # Get the object from MinIO
        data = minio_client.get_object(bucket_name, obj.object_name)
        # Read the object content into a pandas DataFrame
        df = pd.read_csv(BytesIO(data.read()))
        # Append the DataFrame to the list
        dataframes.append(df)
        
        # Close the object
        data.close()
        data.release_conn()
    
    # Concatenate all DataFrames in the list
    if len(dataframes) >0:
        final_df = pd.concat(dataframes, ignore_index=True)
    else:
        final_df = None
    return final_df[['date', 'value']]

