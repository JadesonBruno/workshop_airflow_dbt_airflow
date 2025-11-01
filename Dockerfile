# Base image for Airflow
FROM astrocrpublic.azurecr.io/runtime:3.1-3

# Install dbt and the Postgres adapter in a virtual environment
RUN python -m venv dbt_venv \
    && source dbt_venv/bin/activate \
    && pip install --no-cache-dir \
        dbt-postgres==1.9.0 \
        dbt-redshift==1.9.0 \
    && deactivate
