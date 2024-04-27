import streamlit as st

from pinecone import Pinecone as PineconeSetup

# from langchain_pinecone import PineconeVectorStore
from langchain.embeddings.openai import OpenAIEmbeddings
# from langchain.vectorstores import Pinecone
from langchain_pinecone import Pinecone as VectorStore

from langchain.tools import tool
from langchain_openai import ChatOpenAI

from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

from openai import OpenAI as OpenAIClient
import os
from dotenv import load_dotenv

# load_dotenv()
load_dotenv('/.env')

my_pinecone_api_key = os.getenv('my_pinecone_api_key')
my_openai_api_key = os.getenv('my_openai_api_key')

# from langchain.vectorstores import Pinecone as VectorStore

@tool
def productSummary(input):
    """
    Connects to a Pinecone vector database, retrieves documents based on the query
    embeddings.

    Args:
    input (str): The user's query to be processed.

    Returns:
    results: The most relevent documents in the vector database about products.
    """
    
    # Connect to Pinecone
    api_key = my_pinecone_api_key
    pinecone = PineconeSetup(api_key=api_key)

    index = pinecone.Index('customers')
    print(index.describe_index_stats())
    vectorstore = VectorStore.from_existing_index(index_name='customers', embedding=OpenAIEmbeddings(openai_api_key=my_openai_api_key, model="text-embedding-3-small"))
    documents = vectorstore.similarity_search(query=input)
    print(documents)
    return documents

prompt = ChatPromptTemplate.from_messages([
    ("system", "You're a helpful product summary assistant"), 
    ("human", "{input}"), 
    ("placeholder", "{agent_scratchpad}"),
])

model = ChatOpenAI(model='gpt-3.5-turbo-0125', openai_api_key=my_openai_api_key)
tools=[productSummary]

agent = create_tool_calling_agent(model, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Streamlit interface
st.title('LangChain Chatbot Example')
user_input = st.text_input("This chatbot is really good at 2 things: 1. Giving product summaries and 2. Giving buyer summaries", "")
if user_input:
    response = agent_executor.invoke(
        {
            "input": (user_input)
        }
    )
    st.write(response)
    print("Agent response:", response)  # Debugging