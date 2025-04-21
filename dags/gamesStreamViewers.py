from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

# ========== LOAD ENV VARIABLES ==========
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# ========== DEFAULT ARGS ==========
default_args = {
    "owner": "airflow",
}

# ========== DAG SETUP ==========
dag = DAG(
    dag_id="game_average_view_count_dag",
    default_args=default_args,
    description="Calculate average viewer count for each game and store it in Neo4j",
    catchup=False,
)

# ========== FUNCTIONS ==========

def calculate_and_store_averages(**kwargs):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # Fetch all game names (or IDs)
        result = session.run("""
            MATCH (g:Game)-[:HAS_STREAM]->(s:Stream)
            RETURN g.name AS game_name, avg(s.viewer_count) AS avg_viewers
        """)

        for record in result:
            game_name = record["game_name"]
            avg_viewers = record["avg_viewers"]

            if avg_viewers is not None:
                session.run("""
                    MATCH (g:Game {name: $game_name})
                    MERGE (a:Average_View_Count {value: $avg_viewers})
                    MERGE (g)-[:HAS_AVERAGE_VIEW_COUNT]->(a)
                """, game_name=game_name, avg_viewers=avg_viewers)
                print(f"Processed: {game_name} with avg viewers: {avg_viewers}")

                # session.run("""
                #     MATCH (g:Game {name: $game_name})
                #     OPTIONAL MATCH (g)-[r:HAS_AVERAGE_VIEW_COUNT]->(old:Average_View_Count)
                #     DELETE r
                #     WITH old
                #     WHERE old IS NOT NULL
                #     DETACH DELETE old
                #     CREATE (a:Average_View_Count {value: $avg_viewers})
                #     MERGE (g)-[:HAS_AVERAGE_VIEW_COUNT]->(a)
                # """, game_name=game_name, avg_viewers=avg_viewers)
                # print(f"Processed: {game_name} with avg viewers: {avg_viewers}")
                    
    driver.close()

# ========== TASK ==========
process_avg_view_counts = PythonOperator(
    task_id="calculate_and_store_averages",
    python_callable=calculate_and_store_averages,
    provide_context=True,
    dag=dag,
)

process_avg_view_counts