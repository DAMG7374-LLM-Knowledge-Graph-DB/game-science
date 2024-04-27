from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import snowflake.connector
from openai import OpenAI
from pinecone import Pinecone
import os
from dotenv import load_dotenv

load_dotenv()

my_pinecone_api_key = os.getenv('my_pinecone_api_key')
my_openai_api_key = os.getenv('my_openai_api_key')

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
    'persona_dag',
    default_args=default_args,
    description='Perform tasks on new company additions in Snowflake',
)

def insert_persona_pinecone():
    # Fetch data from Snowflake
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()
    cursor.execute("SELECT UID, PERSONA FROM TRANSFORM_DB.TRANSFORM_SCHEMA_2.PERSONAS")
    uid_persona_descriptions = {row[0]: row[1] for row in cursor.fetchall()}  # Creating a dictionary of UID to persona description

    # Connect to Pinecone
    pinecone = Pinecone(api_key=my_pinecone_api_key)

    # Create or update the index in Pinecone
    index_name = pinecone.Index("customers")
    # index_name = "customers"

    openai_api_key = my_openai_api_key
    client = OpenAI(api_key=openai_api_key,)

    # Insert or update persona descriptions into Pinecone based on UID
    # for uid, persona_description in uid_persona_descriptions.items():
    #     pinecone.upsert(index_name, [uid], [persona_description])

    # Upsert persona descriptions into Pinecone based on UID
    for uid, persona_description in uid_persona_descriptions.items():
        persona_description_vector = client.embeddings.create(model="text-embedding-3-small", input=[persona_description]).data[0].embedding
        print(persona_description_vector)
        upsert_response = index_name.upsert(
            vectors=[
                {
                    "id": str(uid),
                    "values": persona_description_vector,
                    "metadata": {
                        "description": persona_description
                    }
                }
            ],
            namespace="Company-Vector"
        )

    # Close connections
    cursor.close()
    connection.close()

insert_persona_pinecone_task = PythonOperator(
    task_id='insert_persona_pinecone',
    python_callable=insert_persona_pinecone,
    provide_context=True,
    dag=dag,
)

def insert_persona_snowflake(**context):
    personas = context['task_instance'].xcom_pull(task_ids='create_ai_persona')
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    for uid, aiPersona in personas.items():    
        # sql = "UPDATE TRANSFORM_DB.TRANSFORM_SCHEMA_2.PERSONAS cd SET persona = %s, personaCreateDate = CURRENT_TIMESTAMP() WHERE UID = %s"
        sql = """
            MERGE INTO TRANSFORM_DB.TRANSFORM_SCHEMA_2.PERSONAS p
            USING (
                SELECT %s AS persona, CURRENT_TIMESTAMP() AS personaCreateDate, %s AS UID
            ) s
            ON p.UID = s.UID
            WHEN MATCHED THEN
                UPDATE SET
                    p.persona = s.persona,
                    p.personaCreateDate = s.personaCreateDate
            WHEN NOT MATCHED THEN
                INSERT (persona, personaCreateDate, UID)
                VALUES (s.persona, s.personaCreateDate, s.UID);
            """
        cursor.execute(sql, (aiPersona, uid))
        connection.commit()

    connection.close()

insert_persona_snowflake_task = PythonOperator(
    task_id='insert_persona_snowflake',
    python_callable=insert_persona_snowflake,
    provide_context=True,
    dag=dag,
)

def getCompanyPersona(**context):
    dictionaryOfCompanies = context['task_instance'].xcom_pull(task_ids='create_key_value_pairs')
    scrapped_data = {}

    for uid, rawPersona in dictionaryOfCompanies.items():
        data_str = str(f"Company ID: {uid}, Persona: {rawPersona}")

        # Prompt for GPT-2
        prompt = "You are tasked with parsing a key value object containing attributes about a company and then synthesizing a buyer persona out of it in paragraph form. One attribute called SCRAPPEDDATA contains extracted text from each company's website. Here is the object to parse (please don't mention the words key, value, SCRAPPEDDATA, persona, or company id in the output): {}".format(data_str)
                
        openai_api_key = my_openai_api_key

        client = OpenAI(api_key=openai_api_key,)
            
        response = client.completions.create(
        model="gpt-3.5-turbo-instruct",
        prompt=prompt,
        max_tokens=1000,
        temperature=0.7,
        )

        # Extract the generated text (order type) from the response
        aiPersona = response.choices[0].text.strip()
        scrapped_data[uid] = aiPersona
        # data_str[str(company)] = aiPersona
        
    print(scrapped_data)
    return scrapped_data


# Task to scrape website for basic description
ai_persona_task = PythonOperator(
    task_id='create_ai_persona',
    python_callable=getCompanyPersona,
    provide_context=True,
    dag=dag,
)

def create_key_value_pairs():
    connection = snowflake.connector.connect(**snowflake_params)
    cursor = connection.cursor()

    # query = 'SELECT * from TRANSFORM_DB.TRANSFORM_SCHEMA_2.COMPANIESDEDUPEDDATA cd WHERE DATE(SCRAPPEDDATE) BETWEEN DATEADD(DAY, -7, CURRENT_DATE) AND CURRENT_DATE LIMIT 300'
    query = """
    SELECT * 
    FROM TRANSFORM_DB.TRANSFORM_SCHEMA_2.COMPANIESDEDUPEDDATA cd 
    WHERE DATE(SCRAPPEDDATE) BETWEEN DATEADD(DAY, -7, CURRENT_DATE) AND CURRENT_DATE
    AND NOT EXISTS (
        SELECT 1
        FROM TRANSFORM_DB.TRANSFORM_SCHEMA_2.PERSONAS p
        WHERE p.UID = cd.UID
    )"""
  
    cursor.execute(query)
    result = cursor.fetchall()

    # Create a dictionary to store key-value pairs
    key_value_pairs = {}

    # Extract values from each row and store in the dictionary
    for row in result:
        row_dict = {}
        for i, column in enumerate(cursor.description):
            row_dict[column[0]] = row[i]
        # Assuming 'uid' is the name of the unique ID column in the table
        key_value_pairs[row_dict['UID']] = row_dict

    print(key_value_pairs)
    return key_value_pairs

key_value_task = PythonOperator(
    task_id='create_key_value_pairs',
    python_callable=create_key_value_pairs,
    dag=dag,
)

# Define task dependencies
key_value_task >> ai_persona_task >> insert_persona_snowflake_task >> insert_persona_pinecone_task
