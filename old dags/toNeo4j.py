import boto3
import pypdf
from transformers import pipeline
from neo4j import GraphDatabase

# Initialize NER pipeline
ner_pipeline = pipeline("ner", model="dbmdz/bert-large-cased-finetuned-conll03-english")

def download_pdf_from_s3(bucket_name, object_name, download_path):
    """
    Downloads a PDF file from an S3 bucket to local path.
    """
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket_name, object_name, download_path)
    print("Downloaded PDF from S3")

def extract_text_from_pdf(pdf_path):
    """
    Extracts all text from a PDF using pypdf.
    """
    reader = pypdf.PdfReader(pdf_path)
    return "\n".join(page.extract_text() for page in reader.pages if page.extract_text())

def extract_entities(text):
    """
    Runs Named Entity Recognition (NER) on text to extract entities.
    """
    return ner_pipeline(text)

def create_or_update_neo4j_graph(entities, uri, username, password):
    """
    Updates an existing Neo4j Aura graph with entities. Supports dynamic node types.
    Pre-existing node types include Company and Product.
    """
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS NOT NULL")

        for entity in entities:
            name = entity["word"]
            label = entity["entity"].replace("B-", "").replace("I-", "")  # e.g., ORG, PRODUCT, etc.

            # Map known types to graph-specific labels
            if label == "ORG":
                node_label = "Company"
            elif label == "PRODUCT":
                node_label = "Product"
            else:
                node_label = label.capitalize()  # fallback: Person, Location, etc.

            # Merge node
            session.run(
                f"MERGE (n:{node_label} {{name: $name}})",
                name=name
            )

        # Optionally: Define basic heuristic relationships
        # e.g., if both Company and Product found in same document, connect with SELLS
        companies = [e['word'] for e in entities if 'ORG' in e['entity']]
        products = [e['word'] for e in entities if 'PRODUCT' in e['entity']]
        for company in companies:
            for product in products:
                session.run("""
                    MATCH (c:Company {name: $company}), (p:Product {name: $product})
                    MERGE (c)-[:SELLS]->(p)
                """, company=company, product=product)

    print("Neo4j graph updated.")

def main():
    # AWS S3
    # bucket_name = 'damg-final-project'
    # object_name = 'apple.pdf'
    # download_path = 'apple.pdf'

    # Neo4j Aura credentials
    uri = "neo4j+s://<iY9s2QT71Dp79wXNJ4H904sMm3-8WSxoOgXnGQPwZy4>.databases.neo4j.io"
    username = "<farisDAMG99"
    password = "<iY9s2QT71Dp79wXNJ4H904sMm39>"

    # Pipeline
    download_pdf_from_s3(bucket_name, object_name, download_path)
    text = extract_text_from_pdf(download_path)
    entities = extract_entities(text)
    create_or_update_neo4j_graph(entities, uri, username, password)

if __name__ == "__main__":
    main()