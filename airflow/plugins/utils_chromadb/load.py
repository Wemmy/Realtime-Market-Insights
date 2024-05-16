import openai
from airflow.models import Variable
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.core import Settings,StorageContext,VectorStoreIndex
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core.ingestion import IngestionPipeline
from llama_index.core.node_parser import SentenceSplitter
import chromadb
openai.api_key = Variable.get("OPENAI_API_KEY", default_var=None)

minio_config = {
    'endpoint': 'minio:9000',
    'access_key': 'admin',
    'secret_key': 'password',
    'secure': False
}

# # global
# Settings.embed_model = OpenAIEmbedding()

def load_data(docs, name_collection):
    # create the pipeline with transformations
    pipeline = IngestionPipeline(
        transformations=[
            SentenceSplitter(chunk_size=1024, chunk_overlap=20),
            OpenAIEmbedding(model='text-embedding-ada-002', embed_batch_size=100),
        ]
    )
    # create noeds
    nodes = pipeline.run(documents=docs)
    # build vector store

    chroma_client = chromadb.HttpClient(host='chroma',port = 8000)
    chroma_collection = chroma_client.get_or_create_collection(name_collection)
    vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
    # start ingestion
    '''need to think about how to avoid adding duplicated docs'''
    vector_store.add(nodes)