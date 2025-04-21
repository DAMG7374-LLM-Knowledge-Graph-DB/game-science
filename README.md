## Buyer Data Pipeline 

For the buyer pipeline data ingestion, this is the sequence of events: 
Monitor for net new companies in the provider dataset table (In Snowflake) 
Scrape each company’s website 
Store the scrapped data in new Snowflake table  

Our Airflow orchestration comprises three tasks executed in sequential order. Each task gets a unique name and calls a Python function. 

The first task/function directly queries a Snowflake for new records (this requires importing snowflake connector package) 
The second task/function performs web scrapping on each of the company’s websites. This requires 3 packages: request, URL parse and html2text. We also performed exception handling for when a URL cannot be resolved due to HTTP errors. 
The third task/function inserts the scrapped data into a new Snowflake table (also requires the snowflake.connector package) 

These series of operations were performed in Python and orchestrated in Airflow (inside a docker container). Please find this video on Docker/Airflow setup. 2 airflow packages are required (DAG and PythonOperator). These allow the Python functions to become executable/orchestratable tasks on a configured schedule (which we set as nightly).
https://www.youtube.com/watch?v=N3Tdmt1SRTM&pp=ygUUZG9ja2VyIGFpcmZsb3cgc2xlZWs%3D

-----

For the buyer pipeline data TRANSFORMATIONS, this is the sequence of events:
In the transform schema: Stage the raw company data (create a unique ID)  
In the transform schema: Stage the scrapped website data  
Join the restaged records (filter out companies scrapped with HTTP Resolve Errors)  
Filter out duplicate records and store cleaned data in a final transformed table 
We can add filters to based on the country (this is a supplementary view) 
We create a JSON object from each ROW of buyer data in Snowflake (this happens in memory, not stored) 
Use the JSON as an input to GPT4 to generate a buyer persona  
Add that resolved persona to a new snowflake table in the transform schema 
Load the personas to Pinecone vector database, along with its associated unique ID

Let us highlight major technical pieces of our Airflow orchestration for the buyer pipeline data transformation. There are 9 “tasks” that happen in sequential order (see the sequence above). This is encompassed in 2 separate DAGs. Each task gets a unique name and calls a Python function. 

The first task simply copies the raw company data over to the transform schema using DBT 
The second task simply copes the raw website scrapped data over to the transform schema using DBT 
The third task joins the company and website scrapped data based on the “website” key they both share. Again, this is done via DBT and orchestrated in an Airflow task. There is some cleanup via filtration done as well. 
The fourth task removes duplicate company records by checking for unique websites. 
The fifth task filters companies located inside the US. 
The 6th through 9th tasks goes back into a vanilla Airflow DAG (i.e. not using DBT). 
The sixth task loops through each row created in the 4th task and creates a JSON object from all the attributes. 
The seventh task uses GPT4 to create a summary from each input JSON. 
The eighth task pushes that data into a new Snowflake table called “Personas”. 
The final/ninth task also pushes those personas and their Snowflake unique IDs into Pinecone as a vector.

These series of operations were performed in Python and orchestrated in Airflow (inside a docker container). Additionally, Astronomer Cosmos is leveraged to visualize these DBT transformations right inside Airflow. Please find this video on this setup.
https://www.astronomer.io/cosmos/
https://www.youtube.com/watch?v=OLXkGB7krGo&t=20s&ab_channel=jayzern

---

There is a POC Streamlit app as well.