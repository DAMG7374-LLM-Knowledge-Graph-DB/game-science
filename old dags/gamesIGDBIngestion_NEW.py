import os
import json
import requests
from datetime import timedelta
import time
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from neo4j import GraphDatabase
from airflow.utils.dates import days_ago

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
    return response.json()["access_token"]

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
        "fields name, genres.name, genres.id, involved_companies.company.name, "
        "involved_companies.company.id, url, similar_games.name, "
        "game_type, category, franchise, summary, storyline, total_rating, "
        "first_release_date; "
        f"where id = {igdb_id};"
    )
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    return response.json()

def fetch_game_type(game_type_id, access_token):
    url = "https://api.igdb.com/v4/game_types"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}"
    }
    body = f"fields id, name; where checksum = {game_type_id};"
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    result = response.json()
    return result[0] if result else None

def update_neo4j_with_igdb_data(data, access_token):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        igdb_id = data.get("id")

        # 1. Update IGDB_ID node with basic info
        session.run("""
            MATCH (g:IGDB_ID {id: $igdb_id})
            SET g.total_rating = $total_rating,
                g.first_release_date = datetime({epochSeconds: $first_release_date}),
                g.summary = $summary,
                g.url = $url
        """, {
            "igdb_id": igdb_id,
            "total_rating": data.get("total_rating"),
            "first_release_date": data.get("first_release_date") or 0,
            "summary": data.get("summary"),
            "url": data.get("url")
        })

        # 2. Merge franchise
        if data.get("franchise"):
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (f:Franchise {id: $franchise_id})
                MERGE (g)-[:PART_OF_FRANCHISE]->(f)
            """, {
                "igdb_id": igdb_id,
                "franchise_id": data["franchise"]
            })

        # # 3. Merge game_type (via API)
        # if "game_type" in data:
        #     game_type_id = data["game_type"]
        #     try:
        #         game_type = fetch_game_type(game_type_id, access_token)
        #         if game_type:
        #             session.run("""
        #                 MATCH (g:IGDB_ID {id: $igdb_id})
        #                 MERGE (t:GameType {id: $type_id})
        #                 SET t.name = $type_name
        #                 MERGE (g)-[:HAS_TYPE]->(t)
        #             """, {
        #                 "igdb_id": igdb_id,
        #                 "type_id": game_type["checksum"],
        #                 "type_name": game_type["name"]
        #             })
        #     except Exception as e:
        #         logging.warning(f"⚠️ Could not fetch game_type for ID {game_type_id}: {e}")

        # 3. Merge game_type (via API)
        GAME_TYPE_LOOKUP = {
            0: "main_game",
            1: "dlc_addon",
            2: "expansion",
            3: "bundle",
            4: "standalone_expansion",
            5: "mod",
            6: "episode",
            7: "season",
            8: "remake",
            9: "remaster",
            10: "expanded_game",
            11: "port",
            12: "fork",
            13: "pack",
            14: "update"
        }
        game_type_id = data.get("game_type")
        game_type_name = GAME_TYPE_LOOKUP.get(game_type_id)

        if game_type_name:
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (t:GameType {id: $type_id})
                SET t.name = $type_name
                MERGE (g)-[:HAS_TYPE]->(t)
            """, {
                "igdb_id": igdb_id,
                "type_id": game_type_id,
                "type_name": game_type_name
            })
        else:
            print(f"⚠️ Unknown or missing game_type ID {game_type_id} for IGDB ID {igdb_id}")

        # 4. Link to similar games
        for sim_game in data.get("similar_games", []):
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MATCH (s:Game {name: $sim_name})
                MERGE (g)-[:SIMILAR_TO]->(s)
            """, {
                "igdb_id": igdb_id,
                "sim_name": sim_game["name"]
            })

        # 5. Merge genres
        for genre in data.get("genres", []):
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (gn:Genre {id: $genre_id})
                SET gn.name = $genre_name
                MERGE (g)-[:HAS_GENRE]->(gn)
            """, {
                "igdb_id": igdb_id,
                "genre_id": genre["id"],
                "genre_name": genre["name"]
            })

        # 6. Merge companies
        for comp in data.get("involved_companies", []):
            company = comp.get("company")
            if company:
                session.run("""
                    MATCH (g:IGDB_ID {id: $igdb_id})
                    MERGE (c:Company {id: $comp_id})
                    SET c.name = $comp_name
                    MERGE (g)-[:INVOLVES_COMPANY]->(c)
                """, {
                    "igdb_id": igdb_id,
                    "comp_id": company["id"],
                    "comp_name": company["name"]
                })
    driver.close()

def run_igdb_pipeline():
    access_token = get_igdb_access_token()
    igdb_ids = fetch_igdb_ids()

    logging.info(f"✅ Found {len(igdb_ids)} IGDB IDs")
    for igdb_id in igdb_ids:
        try:
            data = fetch_from_igdb(igdb_id, access_token)
            if not data:
                logging.warning(f"⚠️ No data returned for IGDB ID {igdb_id}")
                continue
            update_neo4j_with_igdb_data(data[0], access_token)
            time.sleep(0.4)
        except Exception as e:
            logging.error(f"❌ Error fetching IGDB ID {igdb_id}: {e}")

# Airflow DAG definition
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="fetch_igdb_data_dag_new",
    default_args=default_args,
    catchup=False,
    description="Fetch IGDB data using and enrich Neo4j graph"
) as dag:

    igdb_task = PythonOperator(
        task_id="fetch_and_log_igdb_data_new",
        python_callable=run_igdb_pipeline
    )
