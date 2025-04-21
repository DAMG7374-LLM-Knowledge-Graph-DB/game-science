FROM quay.io/astronomer/astro-runtime:10.5.0
# FROM quay.io/astronomer/astro-runtime:12.7.1

# syntax=quay.io/astronomer/airflow-extensions:latest

# FROM quay.io/astronomer/astro-runtime:9.1.0-python-3.9-base

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && pip install --no-cache-dir dbt-snowflake && deactivate


# Switch to root user
USER root

# Install wkhtmltopdf and xvfb
RUN apt-get update && apt-get install -y wkhtmltopdf xvfb \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow