create virtual env
python -m venv env
.\env\Scripts\activate

ref: https://github.com/confluentinc/cp-all-in-one/blob/7.5.0-post/cp-all-in-one-community/docker-compose.yml

infrustrucre management
docker-compose up airflow-int

docker-compose up

# set up api keys in airflow

# initialize the dataset

1. Deleting the Topic
   The most straightforward method to clear all messages is to delete the topic and recreate it. This action will remove all messages irreversibly. Here’s how you can do this using Kafka's command-line tools:

Change the setting of message retention time
kafka-configs --bootstrap-server your-kafka-server:9092 --entity-type topics --entity-name your-topic-name --alter --add-config retention.ms=86400000

Navigate to your Kafka installation directory and run the following command to delete a topic:
kafka-topics --bootstrap-server broker:29092 --list
kafka-topics --bootstrap-server broker:29092 --delete --topic alpha_vantage_news

reset offset
bin/kafka-consumer-groups --bootstrap-server broker:29092 --group consumer-group-1 --reset-offsets --shift-by -6 --topic yfinance_data -execute --group consumer-group-1


need meta datafilter 
https://docs.llamaindex.ai/en/stable/module_guides/storing/vector_stores/

elasticsearch change password
bin/elasticsearch-reset-password -u elastic -i

      - xpack.security.transport.ssl.enabled=false
      - xpack.security.http.ssl.enabled=false

pgvector with airflow
https://docs.astronomer.io/learn/airflow-pgvector
issue: can not install tensorrt_llm in WSL
