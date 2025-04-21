import os
import requests
import openai
from dotenv import load_dotenv
from datetime import datetime, timedelta
from neo4j import GraphDatabase
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from requests import post

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

IGDB_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
IGDB_AUTH_TOKEN = os.getenv("TWITCH_CLIENT_SECRET")
openai.api_key = os.getenv("OPENAI_API_KEY")

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'neo4j_igdb_summary_pipeline_v2',
    default_args=default_args,
    description='Fetch companies, enrich with IGDB, and summarize site using LLM',
)

def get_igdb_access_token():
    logging.info("🔑 Fetching IGDB access token...")
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": IGDB_CLIENT_ID,
        "client_secret": IGDB_AUTH_TOKEN,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, params=params)
    response.raise_for_status()
    token = response.json()["access_token"]
    logging.info("✅ IGDB token acquired.")
    return token

def fetch_company_nodes(**kwargs):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (c:Company) RETURN c.id AS id")
        companies = [record["id"] for record in result]
    driver.close()
    return companies

def query_igdb(**kwargs):
    access_token = get_igdb_access_token()
    ti = kwargs['ti']
    companies = ti.xcom_pull(task_ids='fetch_company_nodes')
    
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }

    enriched_companies = []
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        response = post('https://api.igdb.com/v4/companies', **{'headers': headers,'data': 'fields change_date,change_date_category,change_date_format,changed_company_id,checksum,country,created_at,description,developed,logo,name,parent,published,slug,start_date,start_date_category,start_date_format,status,updated_at,url,websites;'})
        print ("response: %s" % str(response.json()))
        for company in companies:
            data = f'search "{company}"; fields country, description, url;'
            response = requests.post("https://api.igdb.com/v4/companies", headers=headers, data=data)

            if not response.ok:
                print(f"IGDB error for {company}: {response.text}")
                continue

            results = response.json()
            if not results:
                print(f"No IGDB result for {company}")
                continue

            result = results[0]

            enriched = {
                "id": result.get('id'),
                "description": result.get('description'),
                "url": result.get('url'),
                "country": result.get('country')
            }

            enriched_companies.append(enriched)

            # Update graph
            session.write_transaction(
                update_company_node,
                enriched["id"],
                enriched["description"],
                enriched["url"],
                enriched["country"]
            )

    driver.close()
    ti.xcom_push(key='enriched_companies', value=enriched_companies)

def update_company_node(tx, id, description, url, country):
    tx.run("""
        MATCH (c:Company {id: $id})
        SET c.description = $description
    """, id=id, description=description)

    if url:
        tx.run("""
            MERGE (w:Website {url: $url})
            WITH w
            MATCH (c:Company {id: $id})
            MERGE (c)-[:HAS_WEBSITE]->(w)
        """, id=id, url=url)

    # if country:
    #     tx.run("""
    #         MERGE (country:Country {code: $country})
    #         WITH country
    #         MATCH (c:Company {id: $id})
    #         MERGE (c)-[:LOCATED_IN]->(country)
    #     """, id=id, country=country)

def scrape_and_summarize(**kwargs):
    ti = kwargs['ti']
    companies = ti.xcom_pull(task_ids='query_igdb', key='enriched_companies')

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        for company in companies:
            url = company.get("url")
            if not url:
                continue

            try:
                html = requests.get(url, timeout=10).text
                soup = BeautifulSoup(html, 'html.parser')
                text = soup.get_text()[:2000]

                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": f"Summarize this webpage in 300 max characters:\n{text}"}
                    ]
                )

                summary_text = response['choices'][0]['message']['content']
                timestamp = datetime.utcnow().isoformat()

                session.write_transaction(
                    add_summary_to_website, url, summary_text, "gpt-3.5-turbo", timestamp
                )

            except Exception as e:
                print(f"Error scraping/summarizing {url}: {e}")

    driver.close()

def add_summary_to_website(tx, url, summary_text, model, timestamp):
    tx.run("""
        MERGE (w:Website {url: $url})
        CREATE (s:Summary {
            text: $text,
            model: $model,
            timestamp: $timestamp
        })
        MERGE (w)-[:HAS_SUMMARY]->(s)
    """, url=url, text=summary_text, model=model, timestamp=timestamp)

# Define tasks
fetch_task = PythonOperator(
    task_id='fetch_company_nodes',
    python_callable=fetch_company_nodes,
    provide_context=True,
    dag=dag,
)

igdb_task = PythonOperator(
    task_id='query_igdb',
    python_callable=query_igdb,
    provide_context=True,
    dag=dag,
)

summarize_task = PythonOperator(
    task_id='scrape_and_summarize',
    python_callable=scrape_and_summarize,
    provide_context=True,
    dag=dag,
)

# Set task order
fetch_task >> igdb_task >> summarize_task