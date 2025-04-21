from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
import os
from dotenv import load_dotenv
import pdfkit
from datetime import datetime

load_dotenv()

my_aws_access_key_id = os.getenv('my_aws_access_key_id')
my_aws_secret_access_key = os.getenv('my_aws_secret_access_key')

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'schedule_interval': None,
}

# Define the DAG
dag = DAG(
    'pdf_dag',
    default_args=default_args,
    description='Convert domains to PDFs files, then push to S3 Bucket',
    schedule_interval=None,
    catchup=False
)

# S3 bucket name
s3_bucket_name = 'damg-final-project'

config = pdfkit.configuration(wkhtmltopdf='/usr/bin/wkhtmltopdf')

# Function to convert a domain address into a PDF file
def convertDomainToPDF(url, output_file, **kwargs):
    try:
        pdfkit.from_url(url, output_file, configuration=config)
        print(f"PDF created successfully: {output_file}")
    except Exception as e:
        print(f"Error creating PDF: {e}")

# Function to upload the PDF file to an S3 bucket
def uploadPDFtoS3(file_name, bucket_name, object_name=None, **kwargs):

    print(file_name)
    print(bucket_name)

    # Initialize S3 client
    s3 = boto3.client('s3', aws_access_key_id=my_aws_access_key_id, aws_secret_access_key=my_aws_secret_access_key)
    print(s3.list_buckets())

    # If no object name is specified, use the file name
    if object_name is None:
        object_name = file_name

    # s3_client = boto3.client('s3')

    try:
        # s3_client.upload_file(file_name, bucket_name, object_name)
        s3.upload_file(file_name, bucket_name, object_name)
        print(f"File uploaded to S3: s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

    
convert_domain_to_PDF = PythonOperator(
    task_id='convert_domain_to_PDF_task',
    python_callable=convertDomainToPDF,
    op_args=["https://www.cnn.com/", "cnn.pdf"],
    dag=dag,
)

upload_PDF_to_S3 = PythonOperator(
    task_id='upload_PDF_to_S3_task',
    python_callable=uploadPDFtoS3,
    op_args=["cnn.pdf", s3_bucket_name],
    dag=dag,
)

# Define task dependencies
convert_domain_to_PDF >> upload_PDF_to_S3

# Note: future state is to read multiple domains from snowflake