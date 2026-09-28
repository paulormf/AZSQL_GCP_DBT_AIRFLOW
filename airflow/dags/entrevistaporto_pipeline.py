from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="entrevistaporto_pipeline",
    description="Pipeline incremental SQL Server -> GCS -> BigQuery -> dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["entrevistaporto", "data-engineering"],
) as dag:

    validate_sources = BashOperator(
        task_id="validate_sources",
        bash_command="""
        cd /opt/airflow/project
        python src/discover_tables.py
        """,
    )

    extract_and_load_bronze = BashOperator(
        task_id="extract_and_load_bronze",
        bash_command="""
        cd /opt/airflow/project
        python src/pipeline_incremental.py
        """,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="""
        cd /opt/airflow/project/entrevistaporto_dbt
        dbt build \
          --profiles-dir /opt/airflow/project/config
        """,
    )

    quality_checks = BashOperator(
        task_id="quality_checks",
        bash_command="""
        cd /opt/airflow/project/entrevistaporto_dbt
        dbt test \
          --profiles-dir /opt/airflow/project/config
        """,
    )

    validate_sources >> extract_and_load_bronze >> dbt_build >> quality_checks