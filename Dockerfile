FROM quay.io/astronomer/astro-runtime:10.5.0

# syntax=quay.io/astronomer/airflow-extensions:latest

# FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-snowflake && deactivate