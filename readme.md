 <div align="center">
  <p>
      <img src="https://github.com/Wemmy/Realtime-Market-Insights.git/ETL.png"></a>
  </p>
</div>

# My Setup
- windows 11 with Docker Desktop
- python 3.11
- A charged OpenAI account (free account not workd due to [limit](https://platform.openai.com/settings/organization/limits))

# Infrustructure
```
docker-compose up airflow-int
docker-compose up
```

# set up Open AI keys in airflow

Setup your environment variable in airflow

# Question?

1. need meta datafilter?
https://docs.llamaindex.ai/en/stable/module_guides/storing/vector_stores/

2. how to chage elasticsearch password
bin/elasticsearch-reset-password -u elastic -i

3. pgvector with airflow
https://docs.astronomer.io/learn/airflow-pgvector
issue: can not install tensorrt_llm in WSL
