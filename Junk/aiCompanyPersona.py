from openai import OpenAI
import os

def getCompanyPersona(rawData):
    # Prompt for GPT-2
    prompt = "Please parse the raw text of this company's website and define a buying persona out of it {}?".format(rawData)

    openai_api_key = '123'

    client = OpenAI(api_key=openai_api_key,)
        
    # Call OpenAI's ChatGPT API to get the response
    # response = openai.Completion.create(
    #     engine="text-davinci-002",
    #     prompt=prompt,
    #     # temperature=0.7,
    #     max_tokens=1000,
    #     # n=1,
    #     # stop=None,
    #     api_key=openai_api_key  # Pass the API key to the OpenAI client
    # )

    response = client.completions.create(
    model="gpt-3.5-turbo-instruct",
    prompt=prompt,
    max_tokens=1000,
    temperature=0.7,
    )

    # Extract the generated text (order type) from the response
    aiPersona = response.choices[0].text.strip()

    print(aiPersona)
    return aiPersona


def model(dbt, session):

    dbt.config(packages = ["openai"])
    dbt.config(materialized = "incremental")
    dbt.config(unique_key='uid')
    companiesAIEnriched_df = dbt.ref("companiesScrappedData")

    df = companiesAIEnriched_df.withColumn("AICompanyPersona", getCompanyPersona(companiesAIEnriched_df["SCRAPPEDDATA"]))
    return df
