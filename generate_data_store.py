# from langchain.document_loaders import DirectoryLoader
from langchain_community.document_loaders import DirectoryLoader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.schema import Document
from langchain_openai import OpenAIEmbeddings
from langchain_community.document_loaders import JSONLoader

from sentence_transformers import SentenceTransformer
from langchain_chroma import Chroma 
from langchain_huggingface import HuggingFaceEmbeddings
import openai 

import shutil
from operator import itemgetter

from pprint import pprint
from dotenv import load_dotenv
load_dotenv()
import os
import time
import textwrap
from util import *
import json


# Load environment variables. Assumes that project contains .env file with API keys
load_dotenv()


CHROMA_PATH = "chroma"
DATA_PATH = os.getenv("RAG_LOCAL_DATA_PATH")
embedding_model = get_rag_embedding()
download_s3_bucket_flat( DATA_PATH, "transcriptions")
def main():
    generate_data_store()


def generate_data_store():
    documents = load_documents()
    chunks = split_text(documents)
    save_to_chroma(chunks)



import json
from langchain.docstore.document import Document

def load_json_from_string(json_string):
    """Loads JSON data from a string (assuming it's a single JSON object or JSON Lines)."""
    documents = []
    try:
        # Attempt to load as a single JSON object first
        data = json.loads(json_string)

        # Check if it is a list of objects
        if isinstance(data, list):
          for item in data:
            metadata = {
                "Episode Name": item.get("Episode Name"),
                "Podcast Name": item.get("Podcast Name"),
                "Episode Link": item.get("Episode Link"),
                "duration": item.get("duration"),
                # ... other metadata
            }
            text = item.get("text")
            if text:
              doc = Document(page_content=text, metadata=metadata)
              documents.append(doc)
            else:
              print(f"Warning: No 'text' field found in JSON data: {item}")
        else:
          metadata = {
              "Episode Name": data.get("Episode Name"),
              "Podcast Name": data.get("Podcast Name"),
              "Episode Link": data.get("Episode Link"),
              "duration": data.get("duration"),
              # ... other metadata
          }
          text = data.get("text")
          if text:
            doc = Document(page_content=text, metadata=metadata)
            documents.append(doc)
          else:
            print(f"Warning: No 'text' field found in JSON data: {data}")

    except json.JSONDecodeError as e:
        # If single object parsing fails, try JSON Lines (one object per line)
        try:
            for line in json_string.splitlines():
                if line.strip():  # Skip empty lines
                    data = json.loads(line)
                    metadata = {
                        "Episode Name": data.get("Episode Name"),
                        "Podcast Name": data.get("Podcast Name"),
                        "Episode Link": data.get("Episode Link"),
                        "duration": data.get("duration"),
                        # ... other metadata
                    }
                    text = data.get("text")
                    if text:
                      doc = Document(page_content=text, metadata=metadata)
                      documents.append(doc)
                    else:
                      print(f"Warning: No 'text' field found in JSON data: {data}")

        except json.JSONDecodeError as e2:
            print(f"Error decoding JSON: {e}. Also, unable to decode as JSON Lines: {e2}")
        except Exception as e2:
            print(f"An unexpected error occurred: {e2}. Data: {json_string}")

    except Exception as e:
        print(f"An unexpected error occurred: {e}. Data: {json_string}")

    return documents
# Function to extract metadata
def extract_metadata(json_data, _):
    return {
        "page_content": json_data.get("text", ""),  # Extract transcript text
        "episode_name": json_data.get("Episode Name", ""),
        "podcast_name": json_data.get("Podcast Name", ""),
        "episode_link": json_data.get("Episode Link", ""),
        "duration": json_data.get("duration", ""),
    }
def load_documents():
    log("info", f"Loading Documents  ")
    start_time = time.perf_counter() 
    
    # Use DirectoryLoader to load JSON transcripts
    loader = DirectoryLoader(
        DATA_PATH,
        glob="*.txt",  # JSON files stored with .txt extension
        loader_cls=JSONLoader,
        loader_kwargs={
            "jq_schema": ".",  # Load entire JSON structure
            "text_content": False,  # Use "text" field as the main content
            "metadata_func": extract_metadata,
            },
        
    )
    documents = loader.load()
    for doc in documents:
        doc.page_content = doc.metadata.pop("page_content", "")  # Move text from metadata to content

    end_time = time.perf_counter() 
    elapsed_time = end_time - start_time 
    
    log("info", f"doc loading complete. Took {elapsed_time} seconds")
    return documents


def split_text(documents: list[Document]):
    log("info", f"*** Start Chunking ***")
    start_time = time.perf_counter() 
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=300,
        chunk_overlap=100,
        length_function=len,
        add_start_index=True,
    )
    chunks = text_splitter.split_documents(documents)
    end_time = time.perf_counter() 
    elapsed_time = end_time - start_time 
    log("info", f"Split {len(documents)} documents into {len(chunks)} chunks. Took {elapsed_time} seconds")

    return chunks

def load_sentence_transformer():
    # You can choose any pre-trained model from Sentence-Transformers
    return SentenceTransformer(embedding_model)  # or 'distilbert-base-nli-stsb-mean-tokens'

def save_to_chroma(chunks: list[Document]):
    log("info", f"*** Start Embedding  ***")
    start_time = time.perf_counter() 
    # Clear out the database first.
    if os.path.exists(CHROMA_PATH):
        shutil.rmtree(CHROMA_PATH)

    # Create a new DB from the documents.
    
    if embedding_model != "OPENAI":
        model = load_sentence_transformer()
        embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
    else:
        embeddings = OpenAIEmbeddings()
    

    # Get embeddings for the documents
    db = Chroma.from_documents(
        documents=chunks, 
        embedding=embeddings,
        persist_directory=CHROMA_PATH
    )
    end_time = time.perf_counter() 
    elapsed_time = end_time - start_time 
    log("info", f"Saved {len(chunks)} chunks to {CHROMA_PATH}. Took {elapsed_time} seconds")


if __name__ == "__main__":
    main()