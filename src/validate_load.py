from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import text

from config.extract_config import TABLES_TO_EXTRACT
from discover_tables import create_sql_engine


PROJECT_ID = "botbrewpub-ghsurp"
DATASET_ID = "entrevistaporto_bronze"
SOURCE_DIR = Path("data/raw")


sql_engine = create_sql_engine()
bq_client = bigquery.Client(project=PROJECT_ID)


results = []


print("\nRECONCILIAÇÃO DA CARGA")
print("=" * 80)


for table_name in TABLES_TO_EXTRACT:

    # --------------------------------------------------
    # 1. Azure SQL
    # --------------------------------------------------

    query = text(
        f"SELECT COUNT(*) FROM dbo.{table_name}"
    )

    with sql_engine.connect() as connection:
        azure_count = connection.execute(query).scalar_one()

    # --------------------------------------------------
    # 2. Parquet
    # --------------------------------------------------

    parquet_file = (
        SOURCE_DIR
        / table_name
        / f"{table_name}.parquet"
    )

    parquet_count = len(
        pd.read_parquet(parquet_file)
    )

    # --------------------------------------------------
    # 3. BigQuery
    # --------------------------------------------------

    bq_table = (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{table_name}"
    )

    table = bq_client.get_table(bq_table)

    bq_count = table.num_rows

    # --------------------------------------------------
    # 4. Validação
    # --------------------------------------------------

    status = (
        "OK"
        if azure_count == parquet_count == bq_count
        else "DIVERGÊNCIA"
    )

    results.append(
        {
            "table": table_name,
            "azure": azure_count,
            "parquet": parquet_count,
            "bigquery": bq_count,
            "status": status,
        }
    )

    print(
        f"{table_name:<20}"
        f"{azure_count:>12,}"
        f"{parquet_count:>12,}"
        f"{bq_count:>12,}"
        f"    {status}"
    )


# ------------------------------------------------------
# Resumo
# ------------------------------------------------------

print("\n" + "-" * 80)

total_azure = sum(
    result["azure"]
    for result in results
)

total_parquet = sum(
    result["parquet"]
    for result in results
)

total_bq = sum(
    result["bigquery"]
    for result in results
)

all_ok = all(
    result["status"] == "OK"
    for result in results
)

print(
    f"{'TOTAL':<20}"
    f"{total_azure:>12,}"
    f"{total_parquet:>12,}"
    f"{total_bq:>12,}"
    f"    {'OK' if all_ok else 'DIVERGÊNCIA'}"
)

print("=" * 80)


if not all_ok:
    raise SystemExit(
        "A reconciliação encontrou divergências."
    )

print("\nReconciliação concluída com sucesso.")