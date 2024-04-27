import re
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pinecone import Pinecone
from openai import OpenAI
import boto3
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.document_loaders import PyPDFLoader
from io import BytesIO
from PyPDF2 import PdfReader
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

my_aws_access_key_id = os.getenv('my_aws_access_key_id')
my_aws_secret_access_key = os.getenv('my_aws_secret_access_key')
my_pinecone_api_key = os.getenv('my_pinecone_api_key')
my_openai_api_key = os.getenv('my_openai_api_key')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': None,
}

# Define the DAG
dag = DAG(
    'product_dag',
    default_args=default_args,
    description='Handle new product PDFs',
)

# Initialize S3 client
s3 = boto3.client('s3', aws_access_key_id=my_aws_access_key_id, aws_secret_access_key=my_aws_secret_access_key)

# Connect to Pinecone
pinecone = Pinecone(api_key=my_pinecone_api_key)

# Create or update the index in Pinecone
index_name = pinecone.Index("customers")

# Replace 'your_bucket_name' with your S3 bucket name
bucket_name = 'damg-final-project'

openai_api_key = my_openai_api_key
client = OpenAI(api_key=openai_api_key,)

# Function to upload PDFs from S3 to Pinecone
def pdfsToPinecone():
    # List objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    if 'Contents' in response:
        # Iterate over each object in the bucket
        for obj in response['Contents']:
            # Get the modified date of the file
            modified_date = obj['LastModified'].date()

            # Check if the file was created today or yesterday
            # if modified_date == today or modified_date == yesterday:
            if modified_date is not None:

                # Get the PDF file content
                pdf_content = s3.get_object(Bucket=bucket_name, Key=obj['Key'])['Body'].read()

                # Use PyPDF2 to load the PDF content
                pdf_reader = PdfReader(BytesIO(pdf_content))
                extractedtext = ""
                for page in pdf_reader.pages:
                    extractedtext += page.extract_text()

                # Remove spaces, newlines, and tabs
                extracted_text_final = extractedtext.replace('\n', '').replace('\t', '')
                
                # Split your data up into smaller documents with Chunks
                text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
                texts = text_splitter.split_text(extracted_text_final)
                
                # Create embeddings
                embeddings_list = []
                text_list = []
                for text in texts:
                    res = client.embeddings.create(input=[text], model='text-embedding-3-small')
                    # embeddings_list.append(res['data'][0]['embedding'])
                    embeddings_list.append(res.data[0].embedding)
                    text_list.append(text)

                # Upload the PDF content to Pinecone
                for i, (embedding, text) in enumerate(zip(embeddings_list, text_list)):
                    PDF = obj['Key']

                    upsert_response = index_name.upsert(
                        vectors=[
                            {
                                "id": str(i) + str(PDF),
                                "values": embedding,
                                "metadata": {
                                    "description": text  # You can use the text here if needed
                                }
                            }
                        ],
                        namespace="Product-Vector"
                    )
    
insert_s3_pdf_task = PythonOperator(
    task_id='insert_product_pdfs_to_pinecone',
    python_callable=pdfsToPinecone,
    dag=dag,
)

# Define task dependencies
insert_s3_pdf_task
