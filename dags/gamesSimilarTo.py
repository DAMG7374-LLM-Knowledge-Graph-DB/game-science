import os
import requests
import logging
from dotenv import load_dotenv
from datetime import datetime
from neo4j import GraphDatabase

from airflow import DAG
from airflow.operators.python import PythonOperator

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

# Setup logging
logging.basicConfig(level=logging.INFO)

def get_twitch_token():
    response = requests.post(
        "https://id.twitch.tv/oauth2/token",
        params={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials"
        }
    )
    response.raise_for_status()
    return response.json()["access_token"]

def get_igdb_token():
    return get_twitch_token()  # same as Twitch

def get_similar_games(igdb_id, access_token):
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    body = (
        f"fields similar_games.id, similar_games.name, similar_games.url, similar_games.cover.url;"
        f"where id = {igdb_id};"
    )
    response = requests.post(
        "https://api.igdb.com/v4/games", headers=headers, data=body
    )
    response.raise_for_status()
    return response.json()[0].get("similar_games", [])

def get_igdb_ids_from_neo4j():
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (n:IGDB_ID) RETURN n.id AS id")
        ids = [record["id"] for record in result]
    driver.close()
    return ids

def fetch_and_store_similar_game_data():
    twitch_token = get_twitch_token()
    igdb_token = get_igdb_token()
    igdb_ids = get_igdb_ids_from_neo4j()

    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {twitch_token}"
    }

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    game_query = """
    MERGE (g:Game {IGDB_ID: $igdb_id})
    SET g.name = $name,
        g.box_art_url = $box_art_url

    MERGE (i:IGDB_ID {id: $igdb_id})
    MERGE (g)-[:HAS_IGDB_ID]->(i)
    """

    stream_query = """
    MERGE (s:Stream {id: $stream_id})
    SET s.title = $title,
        s.viewer_count = $viewer_count,
        s.user_name = $user_name

    MERGE (g:Game {name: $game_name})
    MERGE (g)-[:HAS_STREAM]->(s)
    """

    for igdb_id in igdb_ids:
        try:
            similar_games = get_similar_games(igdb_id, igdb_token)
            logging.info(f"🔍 Found {len(similar_games)} similar games for IGDB ID {igdb_id}")

            for game in similar_games:
                game_name = game["name"]
                igdb_id_sim = game["id"]
                box_art_url = game.get("cover", {}).get("url", "")

                # Search Twitch for game info
                twitch_game_resp = requests.get(
                    "https://api.twitch.tv/helix/games",
                    headers=headers,
                    params={"name": game_name}
                )
                twitch_data = twitch_game_resp.json().get("data", [])
                if not twitch_data:
                    logging.warning(f"⚠️ Game '{game_name}' not found on Twitch")
                    continue

                twitch_game = twitch_data[0]
                game_id = twitch_game["id"]

                # Add game + IGDB_ID to Neo4j
                with driver.session() as session:
                    session.run(game_query, {
                        "name": game_name,
                        "box_art_url": box_art_url,
                        # "igdb_id": igdb_id_sim
                        "igdb_id": str(igdb_id_sim)
                    })

                    # Fetch top 3 streams for this game
                    stream_resp = requests.get(
                        "https://api.twitch.tv/helix/streams",
                        headers=headers,
                        params={"game_id": game_id, "first": 3}
                    )
                    streams = stream_resp.json().get("data", [])

                    for stream in streams:
                        session.run(stream_query, {
                            "stream_id": stream["id"],
                            "title": stream["title"],
                            "viewer_count": stream["viewer_count"],
                            "user_name": stream["user_name"],
                            "game_name": game_name
                        })
                        logging.info(f"🎥 Stream '{stream['title']}' added for game '{game_name}'")

        except Exception as e:
            logging.error(f"❌ Error processing IGDB ID {igdb_id}: {e}")

    driver.close()
    logging.info("✅ Finished DAG for similar games and streams.")

# Define DAG
default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="fetch_similar_games_and_streams",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    description="Fetch similar games from IGDB and enrich with Twitch stream data"
) as dag:

    task = PythonOperator(
        task_id="process_similar_games",
        python_callable=fetch_and_store_similar_game_data
    )