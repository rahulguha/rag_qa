import chromadb
from sentence_transformers import SentenceTransformer
import os

# Configuration
chroma_db_path = "chroma_db"
collection_name = "podcast_transcripts"

# Initialize ChromaDB client
client = chromadb.PersistentClient(path=chroma_db_path)

# Load the Chroma collection
collection = client.get_collection(name=collection_name)

# Initialize Sentence Transformer model
model = SentenceTransformer('all-mpnet-base-v2')

def retrieve_relevant_chunks(query, top_k=3):
    """Retrieves the most relevant text chunks from ChromaDB for a given query."""

    query_embedding = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k
    )

    return results['documents'][0]

import os

def generate_answer(query, context_chunks):
    """Generates an answer to the query based on the context chunks."""
    context = os.linesep.join(context_chunks)
    answer = f"Query: {query}{os.linesep}Context:{os.linesep}{context}"
    return answer

# QA loop
while True:
    query = input("Enter your question (or type 'exit' to quit): ")
    if query.lower() == 'exit':
        break

    relevant_chunks = retrieve_relevant_chunks(query)
    answer = generate_answer(query, relevant_chunks)

    print(f"Answer: {answer}
")
