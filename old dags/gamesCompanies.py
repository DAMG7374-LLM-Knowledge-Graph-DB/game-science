import os
import requests
import openai
from dotenv import load_dotenv
from datetime import datetime
from neo4j import GraphDatabase
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Load environment variables from .env
load_dotenv()

# Neo4j and IGDB Configuration from .env
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

IGDB_CLIENT_ID = os.getenv("IGDB_CLIENT_ID")
IGDB_AUTH_TOKEN = os.getenv("IGDB_AUTH_TOKEN")

openai.api_key = os.getenv("my_openai_api_key")

default_args = {
    'owner': 'airflow',
}

dag = DAG(
    'neo4j_igdb_summary_pipeline',
    default_args=default_args,
    description='Fetch companies from Neo4j, enrich via IGDB, summarize via LLM',
)

def fetch_company_nodes(**kwargs):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (c:Company) RETURN c.name AS name")
        companies = [record["name"] for record in result]
    driver.close()
    return companies

def query_igdb(companies, **kwargs):
    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_AUTH_TOKEN}",
    }

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        for company in companies:
            data = f'search "{company}"; fields name, country, description, url;'
            response = requests.post("https://api.igdb.com/v4/companies", headers=headers, data=data)

            if not response.ok:
                print(f"IGDB error for {company}: {response.text}")
                continue

            results = response.json()
            if not results:
                print(f"No IGDB result for {company}")
                continue

            result = results[0]  # Assume best match

            name = result.get('name')
            description = result.get('description')
            url = result.get('url')
            country = result.get('country')  # Should be numeric enum (optional mapping to name?)

            # Update Neo4j node and relationships
            session.write_transaction(update_company_node, name, description, url, country)

    driver.close()

def update_company_node(tx, name, description, url, country):
    # Merge Company node and set properties
    tx.run("""
        MERGE (c:Company {name: $name})
        SET c.description = $description
    """, name=name, description=description)

    # Create or merge Website node and connect
    if url:
        tx.run("""
            MERGE (w:Website {url: $url})
            WITH w
            MATCH (c:Company {name: $name})
            MERGE (c)-[:HAS_WEBSITE]->(w)
        """, name=name, url=url)

    # Country node and relationship
    if country:
        tx.run("""
            MERGE (country:Country {code: $country})
            WITH country
            MATCH (c:Company {name: $name})
            MERGE (c)-[:LOCATED_IN]->(country)
        """, name=name, country=country)

def scrape_and_summarize(**kwargs):
    from datetime import datetime

    ti = kwargs['ti']
    companies = ti.xcom_pull(task_ids='fetch_company_nodes')

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    session = driver.session()

    headers = {
        "Client-ID": IGDB_CLIENT_ID,
        "Authorization": f"Bearer {IGDB_AUTH_TOKEN}",
    }

    for company in companies:
        # Fetch IGDB data to get the URL
        data = f'search "{company}"; fields url;'
        response = requests.post("https://api.igdb.com/v4/companies", headers=headers, data=data)

        if not response.ok:
            print(f"Failed IGDB fetch for {company}")
            continue

        results = response.json()
        if not results or not results[0].get("url"):
            continue

        url = results[0]["url"]

        try:
            html = requests.get(url, timeout=10).text
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text()[:2000]  # Trim to avoid LLM overload

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": f"Summarize this webpage:\n{text}"}
                ]
            )

            summary_text = response['choices'][0]['message']['content']
            timestamp = datetime.utcnow().isoformat()

            session.write_transaction(
                add_summary_to_website, url, summary_text, "gpt-3.5-turbo", timestamp
            )

        except Exception as e:
            print(f"Error summarizing {url}: {e}")

    session.close()
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

fetch_task = PythonOperator(
    task_id='fetch_company_nodes',
    python_callable=fetch_company_nodes,
    provide_context=True,
    dag=dag,
)

igdb_task = PythonOperator(
    task_id='query_igdb',
    python_callable=query_igdb,
    op_args=[ '{{ ti.xcom_pull(task_ids="fetch_company_nodes") }}' ],
    provide_context=True,
    dag=dag,
)

summarize_task = PythonOperator(
    task_id='scrape_and_summarize',
    python_callable=scrape_and_summarize,
    provide_context=True,
    dag=dag,
)

fetch_task >> igdb_task >> summarize_task
