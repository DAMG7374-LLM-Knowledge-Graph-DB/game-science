import os
import json
import requests
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()

# Environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

def get_igdb_access_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, params=params)
    response.raise_for_status()
    token = response.json()["access_token"]
    return token

def fetch_igdb_ids():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (n:IGDB_ID) RETURN n.id AS id")
        ids = [record["id"] for record in result]
    driver.close()
    return ids

def fetch_from_igdb(igdb_id, access_token):
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    body = (
        "fields name, genres.name, involved_companies.company.name, websites.url, "
        "url, similar_games.name, game_type, category, franchise, summary, "
        "storyline, total_rating, first_release_date; "
        f"where id = {igdb_id};"
    )
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    return response.json()

def run_igdb_pipeline():
    access_token = get_igdb_access_token()
    igdb_ids = fetch_igdb_ids()

    print(f"✅ Found {len(igdb_ids)} IGDB IDs")
    for igdb_id in igdb_ids:
        try:
            data = fetch_from_igdb(igdb_id, access_token)
            print(f"🎮 IGDB ID {igdb_id}: {json.dumps(data, indent=2)}")
            time.sleep(0.4)  # Wait 400ms between requests (limit is ~5/sec)
        except Exception as e:
            print(f"❌ Error fetching IGDB ID {igdb_id}: {e}")

# Airflow DAG definition
default_args = {
    "owner": "airflow"
}

with DAG(
    dag_id="fetch_igdb_data_dag",
    default_args=default_args,
    description="Fetch IGDB data using Twitch OAuth2 and Neo4j",
    catchup=False,
    tags=["igdb", "neo4j", "twitch"],
) as dag:

    igdb_task = PythonOperator(
        task_id="fetch_and_log_igdb_data",
        python_callable=run_igdb_pipeline
    )
