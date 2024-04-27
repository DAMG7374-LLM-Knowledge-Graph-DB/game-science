from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import html2text
from html2text import HTML2Text
from airflow.providers.http.hooks.http import HttpHook
import snowflake.connector
import requests
from urllib.parse import urlparse
import os
from dotenv import load_dotenv

load_dotenv()

my_snowflake_password = os.getenv('my_snowflake_password')

snowflake_params = {
    'user': 'sumadif',
    'password': my_snowflake_password,
    'account': 'unb07582.us-east-1',
    'warehouse': 'COMPUTE_WH',
}

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'schedule_interval': None,
}

# Define the DAG
dag = DAG(
    'ingestion_dag',
    default_args=default_args,
    description='Perform tasks on new company additions in Snowflake',
)

def insert_descriptions(**context):
    websitesAndScrappedData = context['task_instance'].xcom_pull(task_ids='scrape_new_websites')
    print("inserting scrapped data per website...")
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    for original_website, scrappedData in websitesAndScrappedData.items():    
        website_without_scheme = original_website.replace('http://', '').replace('https://', '')
        print("Original Website:", website_without_scheme)
        print("Scrapped Data:", scrappedData)
        sql = "INSERT INTO INGESTION.PUBLIC.COMPANY_SCRAPPED_WEB_DATA (website, SCRAPPEDDATA, createDate) VALUES (%s, %s, CURRENT_TIMESTAMP())"
        cursor.execute(sql, (website_without_scheme, scrappedData))
        connection.commit()

    connection.close()

insert_scrappedData_task = PythonOperator(
    task_id='insert_scrapped_data',
    python_callable=insert_descriptions,
    provide_context=True,
    dag=dag,
)

def scrape_websites(**context):
    website_urls = context['task_instance'].xcom_pull(task_ids='monitor_companies_table')
    scrapped_data = {}
    for original_website in website_urls:
        website = original_website
        print("Scraping website: " + website)
        # Check if the URL has a scheme, if not, add 'http://' as the default scheme
        if not urlparse(website).scheme:
            website = 'http://' + website
        try:
            response = requests.get(website, timeout=10)  # Add timeout for requests
            if response.status_code == 200:
                html_content = response.text
                # text_content = html2text.html2text(html_content)
                text_content = html2text.html2text(html_content)[:1000]  # Limit text to first 1000 characters
                # Strip out spaces, newlines, and tabs
                text_content = text_content.replace("\n", "").replace("\t", "")
                scrapped_data[website] = text_content
            else:
                scrapped_data[website] = "HTTP Error: " + str(response.status_code)
        except requests.exceptions.RequestException as e:
            scrapped_data[website] = "Request Error: " + str(e)
        except Exception as e:
            scrapped_data[website] = "Unexpected Error: " + str(e)
    print("Final scrapped data for all websites:")
    print(scrapped_data)
    return scrapped_data

# Task to scrape website for basic description
scrape_website_task = PythonOperator(
    task_id='scrape_new_websites',
    python_callable=scrape_websites,
    # op_kwargs={'website_urls': '{{ task_instance.xcom_pull(task_ids="monitor_companies_table") }}'},
    provide_context=True,
    dag=dag,
)

def fetch_website_urls():
    print("fetching new URLs from the companies table...")
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    query = 'SELECT c.website FROM COMPANIES.PUBLIC.COMPANIES c LEFT JOIN TRANSFORM_DB.TRANSFORM_SCHEMA_2.COMPANIESDEDUPEDDATA cd ON c.website = cd.website WHERE cd.website IS NULL AND c.website IS NOT NULL AND c.country_code IS NOT NULL LIMIT 50;'

    cursor.execute(query)
    result = cursor.fetchall()
    print(result)

    website_urls = [row[0] for row in result]  # Extracting website URLs from the query result
    print(website_urls)
    return website_urls

snowflake_monitor_task = PythonOperator(
    task_id='monitor_companies_table',
    python_callable=fetch_website_urls,
    dag=dag,
)

# Define task dependencies
snowflake_monitor_task >> scrape_website_task >> insert_scrappedData_task
