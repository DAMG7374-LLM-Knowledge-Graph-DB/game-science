import streamlit as st

from pinecone import Pinecone as PineconeSetup

from openai import OpenAI as OpenAIClient

from langchain.llms import OpenAI
from langchain.chains.question_answering import load_qa_chain

from langchain.tools import tool
from langchain_openai import ChatOpenAI

from langchain.agents import create_tool_calling_agent, AgentExecutor
from langchain_core.prompts import ChatPromptTemplate

import os
from dotenv import load_dotenv

# load_dotenv()
load_dotenv('/.env')

my_pinecone_api_key = os.getenv('my_pinecone_api_key')
my_openai_api_key = os.getenv('my_openai_api_key')

@tool
def query_and_respond(input):
    """
    Connects to a Pinecone vector database to retrieve documents based on the query.

    Args:
    input (str): The user's query to be processed.

    Returns:
    formatted_documents: The most relevent documents in the vector database.
    """
    
    # Connect to Pinecone
    api_key = my_pinecone_api_key
    pinecone = PineconeSetup(api_key=api_key)
    # Create or update the index in Pinecone
    index_name = pinecone.Index("customers")

    openai_api_key = my_openai_api_key
    client = OpenAIClient(api_key=openai_api_key)

    vectorizedInput = client.embeddings.create(model="text-embedding-3-small", input=[input]).data[0].embedding
    # print(vectorizedInput)
    docs = index_name.query(vector=vectorizedInput, top_k=3, namespace="Product-Vector", include_metadata=True)
    if 'matches' in docs:
        documents = docs['matches']
        # formatted_documents = [doc['description'] for doc in documents if 'description' in doc]
        formatted_documents = [doc['metadata']['description'] for doc in documents if 'metadata' in doc and 'description' in doc['metadata']]
        # print("Formatted Documents:", formatted_documents)

        # Proceed with using formatted_documents
        # chain_output = chain.run(input_documents=formatted_documents, question=input)
        # # chain_output = chain({"input_documents": formatted_documents, "question": input}, return_only_outputs=True)
        # print("Chain Output:", chain_output)
        # return chain_output
        return formatted_documents
    else:
        print("No documents found or unexpected response structure")
        return "No relevant documents found."

prompt = ChatPromptTemplate.from_messages([
    ("system", "You're a helpful product summary assistant"), 
    ("human", "{input}"), 
    ("placeholder", "{agent_scratchpad}"),
])

model = ChatOpenAI(model='gpt-3.5-turbo-0125', openai_api_key=my_openai_api_key)
tools=[query_and_respond]

agent = create_tool_calling_agent(model, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# Streamlit interface
st.title('LangChain Chatbot Example')
user_input = st.text_input("Ask me anything!", "")
if user_input:
    response = agent_executor.invoke(
        {
            "input": (user_input)
        }
    )
    st.write(response)