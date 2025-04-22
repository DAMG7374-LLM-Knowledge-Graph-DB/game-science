from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import logging

# Load environment variables from .env file
load_dotenv()

# Twitch API credentials
CLIENT_ID = os.getenv('TWITCH_CLIENT_ID')
CLIENT_SECRET = os.getenv('TWITCH_CLIENT_SECRET')

# Neo4j Aura credentials
NEO4J_URI = os.getenv('NEO4J_URI')
NEO4J_USER = os.getenv('NEO4J_USER')
NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')

# Airflow DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}

# Main DAG task function
def fetch_and_merge_games():
    # Step 1: Authenticate with Twitch API to get an access token
    logging.info("Authenticating with Twitch...")
    auth_url = 'https://id.twitch.tv/oauth2/token'
    auth_params = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    auth_response = requests.post(auth_url, params=auth_params)
    access_token = auth_response.json()['access_token']
    logging.info("Twitch access token retrieved.")

    # Prepare headers for Twitch API requests
    headers = {
        'Client-ID': CLIENT_ID,
        'Authorization': f'Bearer {access_token}'
    }

    # Step 2: Fetch top 5 games from Twitch
    logging.info("Fetching top Twitch games...")
    top_games = []
    cursor = None

    while len(top_games) < 10:
        url = 'https://api.twitch.tv/helix/games/top'
        params = {'first': 100 if not cursor else 20, 'after': cursor}
        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        top_games.extend(data['data'])
        cursor = data['pagination'].get('cursor')

        if not cursor:
            break

    top_games = top_games[:10]
    logging.info(f"Retrieved {len(top_games)} top games.")

    # Step 3: Connect to Neo4j Aura
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    # Cypher query to merge Game and IGDB_ID nodes
    # game_query = """
    # MERGE (g:Game {name: $name})
    # SET g.box_art_url = $box_art_url

    # MERGE (i:IGDB_ID {id: $id})
    # MERGE (g)-[:HAS_IGDB_ID]->(i)
    # """

    game_query = """
    MERGE (g:Game {name: $name})
    SET g.box_art_url = $box_art_url,
        g.IGDB_ID = $id

    MERGE (i:IGDB_ID {id: $id})
    MERGE (g)-[:HAS_IGDB_ID]->(i)
    """

    # Cypher query to merge Streams nodes and relate them to their Game
    streams_query = """
    MERGE (b:Stream {id: $stream_id})
    SET b.title = $title,
        b.viewer_count = $viewer_count,
        b.user_name = $user_name

    MERGE (g:Game {name: $game_name})
    MERGE (g)-[:HAS_STREAM]->(b)
    """

    # Step 4: Loop over each game and create nodes in Neo4j
    with driver.session() as session:
        for game in top_games:
            logging.info(f"Processing game: {game['name']} (IGDB ID: {game['igdb_id']})")

            # Merge Game and IGDB_ID nodes
            session.run(game_query, {
                "id": game['igdb_id'],
                "name": game['name'],
                "box_art_url": game['box_art_url']
            })

            # Step 5: Fetch streams for the current game
            streams_url = 'https://api.twitch.tv/helix/streams'
            params = {
                'game_id': game['id'],
                'first': 5  # limit to top 5 streams
            }
            streams_response = requests.get(streams_url, headers=headers, params=params)
            streams = streams_response.json().get('data', [])

            logging.info(f" - Found {len(streams)} streams for {game['name']}")

            # Create Stream nodes and relate them to the Game
            for stream in streams:
                session.run(streams_query, {
                    "stream_id": stream['id'],
                    "title": stream['title'],
                    "viewer_count": stream['viewer_count'],
                    "user_name": stream['user_name'],
                    "game_name": game['name']
                })

    driver.close()
    logging.info("Neo4j import completed successfully.")

# Define the DAG
with DAG(
    'fetch_and_merge_twitch_games',
    default_args=default_args,
    description='Fetch top Twitch games and streams and merge into Neo4j Aura',
    catchup=False
) as dag:

    # Define the task
    merge_games_task = PythonOperator(
        task_id='fetch_and_merge_games',
        python_callable=fetch_and_merge_games
    )
