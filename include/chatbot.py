import streamlit as st
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship, Path
from pyvis.network import Network
import streamlit.components.v1 as components
from openai import OpenAI
import tempfile
import os
import pandas as pd
from dotenv import load_dotenv
import random

# --- SETUP ---
load_dotenv()


# Environment variables
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- UTILS ---
client = OpenAI(api_key=OPENAI_API_KEY)

@st.cache_data(show_spinner=False)
def get_schema_summary():
    query = """
    MATCH (a)-[r]->(b)
    RETURN DISTINCT labels(a) AS from, type(r) AS rel_type, labels(b) AS to
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run(query)
        schema_data = []
        for row in result:
            from_labels = row['from'] if row['from'] else []
            to_labels = row['to'] if row['to'] else []
            rel_type = row['rel_type'] if row['rel_type'] else None
            
            if from_labels and to_labels and rel_type:
                schema_data.append({"from": from_labels, "rel_type": rel_type, "to": to_labels})
        return schema_data

@st.cache_data(show_spinner=False)
def get_node_properties_by_label():
    query = """
    CALL db.schema.nodeTypeProperties()
    YIELD nodeType, propertyName
    RETURN nodeType, collect(DISTINCT propertyName) AS properties
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run(query)
        prop_data = {}
        for row in result:
            label = row["nodeType"].replace("`", "")  # Remove backticks
            prop_data[label] = row["properties"]
        return prop_data

import re

# Example: extract all labels, relationship types, and property names from the schema texts
def extract_tokens(schema, properties):
    labels = set(re.findall(r':([A-Z][A-Za-z0-9_]*)', schema))  # e.g., :Movie, :Person
    rels = set(re.findall(r'-\[:([A-Z_][A-Za-z0-9_]*)\]-', schema))  # e.g., [:ACTED_IN]
    props = set(re.findall(r'\.([a-zA-Z_][A-Za-z0-9_]*)', properties))  # e.g., .title, .genre
    return labels, rels, props

def map_to_correct_case(user_input, labels, rels, props):
    corrected = user_input

    def replace_tokens(token_set):
        nonlocal corrected
        for token in token_set:
            pattern = re.compile(r'\b' + re.escape(token.lower()) + r'\b', re.IGNORECASE)
            corrected = pattern.sub(token, corrected)
        return corrected

    corrected = replace_tokens(labels)
    corrected = replace_tokens(rels)
    corrected = replace_tokens(props)
    return corrected

def translate_to_cypher(user_input, schema_text="", node_properties={}):
    properties_text = "\n".join(
        f"{label}: {', '.join(props)}"
        for label, props in node_properties.items()
    )

    labels, rels, props = extract_tokens(schema_text, properties_text)
    normalized_user_input = map_to_correct_case(user_input, labels, rels, props)
    
    prompt = f"""
You are translating a natural language question into a Cypher query that will be visualized as a graph.

**Constraints:**
- Always assign variables to the relationships in the MATCH query.
- Always return the nodes and the relationships between them in your return clause, so that they can be visualized fully.
- Use the variables assigned to the relationships in the MATCH query in the return clause.
- Only use the schema and properties described below to form your query.
- Decide whether to use `=` (exact match) or `CONTAINS` (fuzzy match) based on the user's intent:
    - If the user says "exactly", "named", "called", or quotes a value → use `=`
    - If the user says "containing", "includes", "like", or uses general phrasing → use `CONTAINS`
- Avoid case mismatch issues by using exact casing from the schema.

**Graph Schema:**
{schema_text}

**Node Properties:**
{properties_text}

Now, translate the following question into a Cypher query:

'{normalized_user_input}'
"""
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()

def summarize_result_in_nlp(flattened_data, original_question=""):
    df = pd.DataFrame(flattened_data)
    sample = df.head(10).to_dict(orient='records')  # Limit size for token safety

    prompt = f"""
    Here is the result of a Cypher query based on the user's question: '{original_question}'.

    Summarize the findings in natural language in a clear, concise manner.
    **Do not mention box art, image URLs, or any visual assets.**
    Focus only on game names, genres, relationships, and release information.

    Data:
    {sample}
    """

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2
    )
    return response.choices[0].message.content.strip()


def run_cypher_query(query):
    try:
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        with driver.session() as session:
            result = session.run(query)
            return list(result)
    except Exception as e:
        st.error(f"Error running Cypher query: {e}")
        return []

def flatten_record(record):
    flat = {}
    for key, value in record.items():
        try:
            flat[key] = dict(value.items()) if hasattr(value, "items") else str(value)
        except:
            flat[key] = str(value)
    return flat

def visualize_graph(data):
    net = Network(height="600px", width="100%", notebook=False)
    added_nodes = set()
    pending_edges = []

    label_colors = {}
    color_palette = [
        "#FF5733", "#33FF57", "#3357FF", "#FF33A1", "#A133FF",
        "#33FFF3", "#FFD133", "#FF3333", "#75FF33", "#FF8F33"
    ]

    def get_color(label):
        if label not in label_colors:
            label_colors[label] = color_palette[len(label_colors) % len(color_palette)]
        return label_colors[label]

    for record in data:
        for value in record.values():
            if isinstance(value, Node):
                node_id = str(value.id)
                props = dict(value.items())
                label = props.get("name") or props.get("title") or props.get("id") or (list(value.labels)[0] if value.labels else "Node")
                group = list(value.labels)[0] if value.labels else "Node"
                title = "<br>".join([f"{k}: {v}" for k, v in props.items()])
                if node_id not in added_nodes:
                    net.add_node(node_id, label=label, title=title, color=get_color(group))
                    added_nodes.add(node_id)

            elif isinstance(value, Relationship):
                source = str(value.start_node.id)
                target = str(value.end_node.id)
                rel_type = value.type
                pending_edges.append((source, target, rel_type))

            elif isinstance(value, Path):
                for node in value.nodes:
                    node_id = str(node.id)
                    label = list(node.labels)[0] if node.labels else "Node"
                    props = dict(node.items())
                    title = "<br>".join([f"{k}: {v}" for k, v in props.items()])
                    if node_id not in added_nodes:
                        net.add_node(node_id, label=label, title=title, color=get_color(label))
                        added_nodes.add(node_id)
                for rel in value.relationships:
                    start = str(rel.start_node.id)
                    end = str(rel.end_node.id)
                    rel_type = rel.type
                    pending_edges.append((start, end, rel_type))

    for source, target, rel_type in pending_edges:
        if source in added_nodes and target in added_nodes:
            net.add_edge(source, target, label=rel_type)

    if not added_nodes:
        return None

    net.force_atlas_2based(gravity=-50, central_gravity=0.005, spring_length=200, spring_strength=0.05)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    return temp_file.name

def visualize_schema(schema_data):
    net = Network(height="600px", width="100%", directed=True)
    added_nodes = set()

    label_colors = {}
    color_palette = [
        "#FF5733", "#33FF57", "#3357FF", "#FF33A1", "#A133FF",
        "#33FFF3", "#FFD133", "#FF3333", "#75FF33", "#FF8F33"
    ]

    def get_color(label):
        if label not in label_colors:
            label_colors[label] = color_palette[len(label_colors) % len(color_palette)]
        return label_colors[label]

    for row in schema_data:
        from_label = row["from"][0] if row["from"] else "Unknown"
        to_label = row["to"][0] if row["to"] else "Unknown"
        rel_type = row["rel_type"] if row["rel_type"] else "Unknown"

        if from_label not in added_nodes:
            net.add_node(from_label, label=from_label, color=get_color(from_label))
            added_nodes.add(from_label)
        if to_label not in added_nodes:
            net.add_node(to_label, label=to_label, color=get_color(to_label))
            added_nodes.add(to_label)

        net.add_edge(from_label, to_label, label=rel_type)

    net.force_atlas_2based(gravity=-40, central_gravity=0.01, spring_length=150, spring_strength=0.08)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
    net.save_graph(temp_file.name)
    return temp_file.name

# --- STREAMLIT APP ---

st.set_page_config(page_title="Neo4j NL to Cypher", layout="wide")
st.title("WELCOME TO LUDOPEDIA")

tabs = st.tabs(["🔍 Query Graph", "📚 Graph Schema"])

# --- TAB 1: QUERY GRAPH ---
with tabs[0]:
    schema_data = get_schema_summary()
    node_properties = get_node_properties_by_label()

    schema_text = "\n".join(
        f"{row['from'][0]} -[:{row['rel_type']}]-> {row['to'][0]}"
        for row in schema_data if row['from'] and row['to']
    )

    user_query = st.text_input("Ask something about your graph:")

    cypher_query = ""
    if user_query:
        if st.button("Translate to Cypher"):
            with st.spinner("Translating..."):
                cypher_query = translate_to_cypher(user_query, schema_text, node_properties)
                st.session_state["cypher_query"] = cypher_query

    if "cypher_query" in st.session_state:
        cypher_query = st.session_state["cypher_query"]
        st.write("### Editable Cypher Query")
        edited_query = st.text_area("Edit and run your Cypher query below:", cypher_query, height=150)

        if st.button("Run Edited Query"):
            with st.spinner("Querying Neo4j..."):
                result = run_cypher_query(edited_query)

            if result:
                st.success("Query successful!")

                # Table View
                st.write("### Results (Table View)")
                flattened_data = [flatten_record(dict(r.items())) for r in result]
                st.dataframe(pd.DataFrame(flattened_data))

                # Graph Visualization
                st.write("### Graph Visualization")
                html_path = visualize_graph(result)
                if html_path:
                    components.html(open(html_path, 'r').read(), height=600)
                else:
                    st.warning("⚠️ No nodes or relationships found to visualize.")
                    
                # NLP Summary
                with st.spinner("Summarizing results..."):
                    summary = summarize_result_in_nlp(flattened_data, original_question=user_query)
                    st.write("### 📝 Summary")
                    st.success(summary)
            else:
                st.warning("No data returned from query.")

# --- TAB 2: SCHEMA VISUALIZATION ---
with tabs[1]:
    st.subheader("Graph Schema Overview")
    st.markdown("This diagram shows the possible node types and relationships in your graph schema.")

    schema_vis_path = visualize_schema(schema_data)
    components.html(open(schema_vis_path, 'r').read(), height=600)

    if st.checkbox("Show schema as table"):
        df_schema = pd.DataFrame([
            {"From": row["from"][0] if row["from"] else "", "Relationship": row["rel_type"], "To": row["to"][0] if row["to"] else ""}
            for row in schema_data
        ])
        st.dataframe(df_schema)
