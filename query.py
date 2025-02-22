import argparse
import pprint
# from dataclasses import dataclass
from langchain_chroma import Chroma 
from sentence_transformers import SentenceTransformer
from langchain_openai import OpenAIEmbeddings
from langchain_huggingface import HuggingFaceEmbeddings

from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.memory import ConversationBufferMemory
from langchain.chains import ConversationChain
# from langchain_core.runnables import RunnableWithMessageHistory

from dotenv import load_dotenv
load_dotenv()
import os
from util import *
import json
CHROMA_PATH = "chroma"

PROMPT_TEMPLATE = """
You are a friendly chatbot who answers questions based on the following context only. 
If the context has an Episode Name, summarize the episode. Otherwise answer the question.
If you are summarizing, limit the response to 250 words. Otherwise don't worry about word limit.
Use a playful tone

{context}

---

Answer the question based on the above context: {question}
"""
embedding_model = get_rag_embedding()
def load_sentence_transformer():
    # You can choose any pre-trained model from Sentence-Transformers
    return SentenceTransformer(embedding_model)  # or 'distilbert-base-nli-stsb-mean-tokens'


def main():
    # Create CLI.
    parser = argparse.ArgumentParser()
    parser.add_argument("query_text", type=str, help="The query text.")
    if embedding_model != "OPENAI":
        model = load_sentence_transformer()
        embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
    else:
        embeddings = OpenAIEmbeddings()
    
    db = Chroma(
        embedding_function =embeddings,  # Pass HuggingFaceEmbeddings as 'embedding'
        persist_directory=CHROMA_PATH
    )
    # setup memory
    memory = ConversationBufferMemory(memory_key="history", return_messages=True)
    model = ChatOpenAI(model_name="gpt-4", api_key=os.getenv("OPENAI_API_KEY"))

    conversation_chain = ConversationChain (
        llm=model,
        memory=memory,
        verbose=False  # Set to True to view logs for debugging
    )
    while True:
        # Ask the user for a question
        query_text = input("You: ")

        # Exit the loop if the user types 'quit'
        if query_text.lower() == "quit":
            print("Goodbye!")
            break

        # Search the DB.
        results = db.similarity_search_with_relevance_scores(query_text, k=6)
        
        if len(results) == 0 or results[0][1] < 0.3:
            print(f"Unable to find matching results.")
            return
        context_text = "\n\n---\n\n".join([doc.page_content for doc, _score in results])
        prompt_template = ChatPromptTemplate.from_template(PROMPT_TEMPLATE)

        prompt = prompt_template.format(context=context_text, question=query_text)
        
        # response_text = llm.invoke(prompt)
        sources = [doc.metadata.get("metadata", None) for doc, _score in results]
        for s in results:
            get_source_episode(s[0].metadata)
        # llm_response = response_text.content
        sources = ""
        sources = "\n\n".join([
            f"Podcast Name: {episode.podcast_name}\nEpisode Name: {episode.episode_name}\nEpisode Link: {episode.episode_link}"
            for episode in source_episodes
        ])
        
        # pprint.pprint(formatted_response)

        # Get the model's response based on the conversation chain
        response = conversation_chain.predict(input=prompt)
        formatted_response = f"Response: {response}\nSources: {sources}"
        # Print the response
        print("Bot:", formatted_response)
    

class PodcastEpisode:
    def __init__(self, episode_name: str, episode_link: str, podcast_name: str):
        self.episode_name = episode_name
        self.episode_link = episode_link
        self.podcast_name = podcast_name 

    def __repr__(self):
        return f"PodcastEpisode(episode_name='{self.episode_name}', episode_link='{self.episode_link}', podcast_name='{self.podcast_name}')"

source_episodes = []
# Set to track unique episode names
episode_names_set = set()
def get_source_episode(metadata):
    episode_name = metadata['episode_name']
    if episode_name not in episode_names_set:
        episode = PodcastEpisode(
            episode_name=metadata['episode_name'],
            episode_link=metadata['episode_link'],
            podcast_name=metadata['podcast_name'],
        )
        source_episodes.append(episode)
        episode_names_set.add(episode_name)  # Add the episode name to the set



if __name__ == "__main__":
    main()