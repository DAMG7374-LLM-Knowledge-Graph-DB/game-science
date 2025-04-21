from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import openai
import os
from dotenv import load_dotenv

# ========== LOAD ENVIRONMENT VARIABLES ==========
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

openai.api_key = os.getenv("OPENAI_API_KEY")

# ========== DAG SETUP ==========
default_args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="neo4j_company_summary_dag",
    default_args=default_args,
    description="Fetch companies from Neo4j, generate summaries with OpenAI, and update Neo4j",
    catchup=False,
)

# ========== FUNCTIONS ==========

def fetch_company_names(**kwargs):
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH (c:Company) RETURN c.name AS name")
        companies = [record["name"] for record in result]
        print(companies)
    driver.close()
    kwargs["ti"].xcom_push(key="company_names", value=companies)

def generate_summaries(**kwargs):
    client = openai.OpenAI(api_key=openai.api_key)
    companies = kwargs["ti"].xcom_pull(key="company_names", task_ids="fetch_companies")
    summaries = {}

    for name in companies:
        prompt = f"Give me a 500 character summary about the {name} game company."
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.7,
            )
            summary = response.choices[0].message.content.strip()
            summaries[name] = summary
        except Exception as e:
            summaries[name] = f"Error: {str(e)}"

    kwargs["ti"].xcom_push(key="summaries", value=summaries)
    print(summaries)

# def write_summaries_to_neo4j(**kwargs):
#     summaries = kwargs["ti"].xcom_pull(key="summaries", task_ids="generate_summaries")

#     driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
#     with driver.session() as session:
#         for company_name, summary in summaries.items():
#             session.run("""
#                 MATCH (c:Company {name: $name})
#                 MERGE (s:AI_Company_Summary {summary: $summary})
#                 MERGE (c)-[:HAS_SUMMARY]->(s)
#             """, name=company_name, summary=summary)
#             print(company_name + "" + summary)
#     driver.close()

def write_summaries_to_neo4j(**kwargs):
    summaries = kwargs["ti"].xcom_pull(key="summaries", task_ids="generate_summaries")

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        for company_name, summary in summaries.items():
            session.run("""
                MATCH (c:Company {name: $name})
                OPTIONAL MATCH (c)-[r:HAS_SUMMARY]->(old:AI_Company_Summary)
                DELETE r
                WITH c, old
                MERGE (s:AI_Company_Summary {summary: $summary})
                MERGE (c)-[:HAS_SUMMARY]->(s)
                // Optionally delete orphaned summaries
                WITH old
                WHERE old IS NOT NULL AND NOT (old)<-[:HAS_SUMMARY]-(:Company)
                DELETE old
            """, name=company_name, summary=summary)
            print(company_name + ": " + summary)
    driver.close()

# ========== TASKS ==========

fetch_companies = PythonOperator(
    task_id="fetch_companies",
    python_callable=fetch_company_names,
    provide_context=True,
    dag=dag,
)

generate_summaries = PythonOperator(
    task_id="generate_summaries",
    python_callable=generate_summaries,
    provide_context=True,
    dag=dag,
)

write_to_neo4j = PythonOperator(
    task_id="write_summaries_to_neo4j",
    python_callable=write_summaries_to_neo4j,
    provide_context=True,
    dag=dag,
)

# ========== DAG FLOW ==========
fetch_companies >> generate_summaries >> write_to_neo4j
