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

# 🔍 Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from .env file
load_dotenv()

# Environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

def get_igdb_access_token():
    logging.info("🔑 Fetching IGDB access token...")
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    response = requests.post(url, params=params)
    response.raise_for_status()
    token = response.json()["access_token"]
    logging.info("✅ IGDB token acquired.")
    return token

def fetch_igdb_ids():
    logging.info("🧠 Fetching IGDB IDs from Neo4j...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (n:IGDB_ID) RETURN n.id AS id")
        ids = [record["id"] for record in result]
    driver.close()
    logging.info(f"✅ Retrieved {len(ids)} IGDB IDs: {ids}")
    return ids

def fetch_from_igdb(igdb_id, access_token):
    logging.info(f"📦 Fetching IGDB data for ID: {igdb_id}")
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    }
    body = (
        "fields name, genres.name, genres.id, involved_companies.company.name, "
        "involved_companies.company.id, url, similar_games.id, "
        "game_type, category, franchise, summary, storyline, total_rating, "
        "first_release_date; "
        f"where id = {igdb_id};"
    )
    response = requests.post(url, headers=headers, data=body)
    response.raise_for_status()
    data = response.json()
    logging.info(f"📥 IGDB returned data for ID {igdb_id}: {json.dumps(data, indent=2)}")
    return data

def update_neo4j_with_igdb_data(data, access_token):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        # igdb_id = data.get("id")
        igdb_id = str(data.get("id"))  # 👈 Ensure string type
        game_name = data.get("name")
        logging.info(f"🔍 Game name from IGDB: {game_name}")
        logging.info(f"🛠 Updating Neo4j node for IGDB ID: {igdb_id}")

        # 1. Update IGDB_ID node with basic info
        session.run("""
            MATCH (g:IGDB_ID {id: $igdb_id})
            SET g.total_rating = $total_rating,
                g.first_release_date = datetime({epochSeconds: $first_release_date}),
                g.summary = $summary,
                g.url = $url,
                g.payload = $payload
        """, {
            "igdb_id": igdb_id,
            "total_rating": data.get("total_rating"),
            "first_release_date": data.get("first_release_date") or 0,
            "summary": data.get("summary"),
            "url": data.get("url"),
            "payload": str(data)
        })
        logging.info("✅ Updated IGDB_ID node with base info.")

        # 1.5. Merge Year node and link it
        release_ts = data.get("first_release_date")
        if release_ts:
            release_year = time.gmtime(release_ts).tm_year
            session.run("""
                MERGE (y:Release_Year {year: $year})
                WITH y
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (g)-[:RELEASED_IN]->(y)

                WITH y
                MATCH (gm:Game {IGDB_ID: $igdb_id})
                MERGE (gm)-[:RELEASED_IN]->(y)
            """, {
                "igdb_id": igdb_id,
                "year": release_year
            })
            logging.info(f"📆 Linked IGDB ID {igdb_id} and game to year {release_year}")

        # 2. Merge franchise
        # if data.get("franchise"):
        #     session.run("""
        #         MATCH (g:IGDB_ID {id: $igdb_id})
        #         MERGE (f:Franchise {id: $franchise_id})
        #         MERGE (g)-[:PART_OF_FRANCHISE]->(f)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "franchise_id": data["franchise"]
        #     })
        #     session.run("""
        #         MATCH (f:Franchise {id: $franchise_id})
        #         MATCH (gm:Game {name: $game_name})
        #         MERGE (gm)-[:PART_OF_FRANCHISE]->(f)
        #     """, {
        #         "franchise_id": data["franchise"],
        #         "game_name": game_name
        #     })
        #     logging.info(f"🔗 Linked to Franchise ID {data['franchise']}")
        if data.get("franchise"):
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (f:Franchise {id: $franchise_id})
                MERGE (g)-[:PART_OF_FRANCHISE]->(f)

                WITH f
                MATCH (gm:Game {IGDB_ID: $igdb_id})
                MERGE (gm)-[:PART_OF_FRANCHISE]->(f)
            """, {
                "igdb_id": igdb_id,
                "franchise_id": data["franchise"]
            })

        # 3. Merge game_type
        GAME_TYPE_LOOKUP = {
            0: "main_game", 1: "dlc_addon", 2: "expansion", 3: "bundle",
            4: "standalone_expansion", 5: "mod", 6: "episode", 7: "season",
            8: "remake", 9: "remaster", 10: "expanded_game", 11: "port",
            12: "fork", 13: "pack", 14: "update"
        }
        game_type_id = data.get("game_type")
        game_type_name = GAME_TYPE_LOOKUP.get(game_type_id)

        # if game_type_name:
        #     session.run("""
        #         MATCH (g:IGDB_ID {id: $igdb_id})
        #         MERGE (t:GameType {id: $type_id})
        #         SET t.name = $type_name
        #         MERGE (g)-[:HAS_TYPE]->(t)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "type_id": game_type_id,
        #         "type_name": game_type_name
        #     })
        #     session.run("""
        #         MATCH (t:GameType {id: $type_id})
        #         MATCH (gm:Game {name: $game_name})
        #         MERGE (gm)-[:HAS_TYPE]->(t)
        #     """, {
        #         "type_id": game_type_id,
        #         "game_name": game_name
        #     })
        if game_type_name:
            session.run("""
            MATCH (g:IGDB_ID {id: $igdb_id})
            MERGE (t:GameType {id: $type_id})
            SET t.name = $type_name
            MERGE (g)-[:HAS_TYPE]->(t)

            WITH t
            MATCH (gm:Game {IGDB_ID: $igdb_id})
            MERGE (gm)-[:HAS_TYPE]->(t)
        """, {
            "igdb_id": igdb_id,
            "type_id": game_type_id,
            "type_name": game_type_name
            })
            logging.info(f"🧩 Linked game type '{game_type_name}' to IGDB ID {igdb_id}")
        else:
            logging.warning(f"⚠️ Unknown or missing game_type ID {game_type_id} for IGDB ID {igdb_id}")

        # 4. Link to similar games
        # for sim_game in data.get("similar_games", []):
            # session.run("""
            #     MATCH (g:IGDB_ID {id: $igdb_id})
            #     MATCH (s:Game {name: $sim_name})
            #     MERGE (g)-[:SIMILAR_TO]->(s)
            # """, {
            #     "igdb_id": igdb_id,
            #     "sim_name": sim_game["name"]
            # })
            # session.run("""
            #     MATCH (gm:Game {name: $game_name})
            #     MATCH (s:Game {name: $sim_name})
            #     MERGE (gm)-[:SIMILAR_TO]->(s)
            # """, {
            #     "game_name": game_name,
            #     "sim_name": sim_game["name"]
            # })
        # for sim_game in data.get("similar_games", []):
        #     session.run("""
        #         MATCH (g:IGDB_ID {id: $igdb_id})
        #         MATCH (s:Game {IGDB_ID: $similar_id})
        #         MERGE (g)-[:SIMILAR_TO]->(s)

        #         WITH s
        #         MATCH (gm:Game {IGDB_ID: $igdb_id})
        #         MERGE (gm)-[:SIMILAR_TO]->(s)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "similar_id": sim_game["id"]
        #     })
        #     logging.info(f"🔗 Linked similar game: {sim_game['id']}")
        # for sim_game in data.get("similar_games", []):
        #     session.run("""
        #         MATCH (g:IGDB_ID {id: $igdb_id})
        #         MATCH (s:Game {IGDB_ID: $similar_id})
        #         MERGE (g)-[:SIMILAR_TO]->(s)

        #         WITH s
        #         MATCH (gm:Game {IGDB_ID: $igdb_id})
        #         MERGE (gm)-[:SIMILAR_TO]->(s)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "similar_id": sim_game["id"]
        #     })
        #     # Link Game <-> Similar Game (bidirectional)
        #     session.run("""
        #         MATCH (gm:Game {IGDB_ID: $igdb_id})
        #         MATCH (s:Game {IGDB_ID: $similar_id})
        #         MERGE (gm)-[:SIMILAR_TO]->(s)
        #         MERGE (s)-[:SIMILAR_TO]->(gm)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "similar_id": sim_game["id"]
        #     })
        #     logging.info(f"🔗 Linked similar game: {igdb_id} <--> {sim_game['id']}")
        for sim_game in data.get("similar_games", []):
            session.run("""
                MATCH (gm:Game {IGDB_ID: $igdb_id})
                MATCH (s:Game {IGDB_ID: $similar_id})
                
                MERGE (gm)-[:SIMILAR_TO]->(s)
            """, {
                # "igdb_id": igdb_id,
                # "similar_id":  sim_game["id"]
                "igdb_id": str(igdb_id),
                "similar_id": str(sim_game["id"])
            })
            logging.info(f"🔗 Linked similar game: {igdb_id} <--> {sim_game['id']}")

        # 5. Merge genres
        # for genre in data.get("genres", []):
        #     session.run("""
        #         MATCH (g:IGDB_ID {id: $igdb_id})
        #         MERGE (gn:Genre {id: $genre_id})
        #         SET gn.name = $genre_name
        #         MERGE (g)-[:HAS_GENRE]->(gn)
        #     """, {
        #         "igdb_id": igdb_id,
        #         "genre_id": genre["id"],
        #         "genre_name": genre["name"]
        #     })
        #     session.run("""
        #         MATCH (gn:Genre {id: $genre_id})
        #         MATCH (gm:Game {name: $game_name})
        #         MERGE (gm)-[:HAS_GENRE]->(gn)
        #     """, {
        #         "genre_id": genre["id"],
        #         "game_name": game_name
        #     })
        for genre in data.get("genres", []):
            session.run("""
                MATCH (g:IGDB_ID {id: $igdb_id})
                MERGE (gn:Genre {id: $genre_id})
                SET gn.name = $genre_name
                MERGE (g)-[:HAS_GENRE]->(gn)

                WITH gn
                MATCH (gm:Game {IGDB_ID: $igdb_id})
                MERGE (gm)-[:HAS_GENRE]->(gn)
            """, {
                "igdb_id": igdb_id,
                "genre_id": genre["id"],
                "genre_name": genre["name"]
            })
            logging.info(f"🎨 Linked genre: {genre['name']}")

        # 6. Merge companies
        # for comp in data.get("involved_companies", []):
        #     company = comp.get("company")
        #     if company:
        #         session.run("""
        #             MATCH (g:IGDB_ID {id: $igdb_id})
        #             MERGE (c:Company {id: $comp_id})
        #             SET c.name = $comp_name
        #             MERGE (g)-[:INVOLVES_COMPANY]->(c)
        #         """, {
        #             "igdb_id": igdb_id,
        #             "comp_id": company["id"],
        #             "comp_name": company["name"]
        #         })
        #         session.run("""
        #             MATCH (c:Company {id: $comp_id})
        #             MATCH (gm:Game {name: $game_name})
        #             MERGE (gm)-[:INVOLVES_COMPANY]->(c)
        #         """, {
        #             "comp_id": company["id"],
        #             "game_name": game_name
        #         })
        for comp in data.get("involved_companies", []):
            company = comp.get("company")
            if company:
                session.run("""
                    MATCH (g:IGDB_ID {id: $igdb_id})
                    MERGE (c:Company {id: $comp_id})
                    SET c.name = $comp_name
                    MERGE (g)-[:INVOLVES_COMPANY]->(c)

                    WITH c
                    MATCH (gm:Game {IGDB_ID: $igdb_id})
                    MERGE (gm)-[:INVOLVES_COMPANY]->(c)
                """, {
                    "igdb_id": igdb_id,
                    "comp_id": company["id"],
                    "comp_name": company["name"]
                })
                logging.info(f"Linked company: {company['name']}")
    driver.close()
    logging.info(f"✅ Finished updating graph for IGDB ID: {igdb_id}")

def run_igdb_pipeline():
    access_token = get_igdb_access_token()
    igdb_ids = fetch_igdb_ids()

    logging.info(f"🚀 Starting pipeline for {len(igdb_ids)} IGDB IDs...")
    for igdb_id in igdb_ids:
        try:
            data = fetch_from_igdb(igdb_id, access_token)
            if not data:
                logging.warning(f"⚠️ No data returned for IGDB ID {igdb_id}")
                continue
            update_neo4j_with_igdb_data(data[0], access_token)
            time.sleep(0.4)
        except Exception as e:
            logging.error(f"❌ Error processing IGDB ID {igdb_id}: {e}")

# Airflow DAG definition
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="fetch_igdb_data_dag_new2",
    default_args=default_args,
    catchup=False,
    description="Fetch IGDB data using and enrich Neo4j graph"
) as dag:

    igdb_task = PythonOperator(
        task_id="fetch_and_log_igdb_data_new",
        python_callable=run_igdb_pipeline
    )