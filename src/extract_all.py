from pathlib import Path

import pandas as pd

from config.extract_config import TABLES_TO_EXTRACT
from discover_tables import create_sql_engine, discover_tables


# ============================================================
# Configuração
# ============================================================

SCHEMA = "dbo"
OUTPUT_DIR = Path("data/raw")


# ============================================================
# Conexão
# ============================================================

engine = create_sql_engine()


# ============================================================
# Discovery
# ============================================================

available_tables = discover_tables(
    engine,
    schema=SCHEMA,
)


# ============================================================
# Validação das tabelas solicitadas
# ============================================================

requested_tables = set(TABLES_TO_EXTRACT)
available_table_names = set(available_tables.keys())

missing_tables = requested_tables - available_table_names

if missing_tables:

    print("\nERRO: tabelas solicitadas não encontradas:")

    for table in sorted(missing_tables):
        print(f"  - {table}")

    raise SystemExit(1)


# ============================================================
# Extração
# ============================================================

print(
    f"\nTabelas solicitadas para extração: "
    f"{len(TABLES_TO_EXTRACT)}"
)

for table_name in TABLES_TO_EXTRACT:

    config = available_tables[table_name]

    print(f"\nExtraindo tabela: {table_name}")

    columns = ",\n            ".join(
        config["columns"]
    )

    query = f"""
        SELECT
            {columns}
        FROM {config["source_table"]}
        ORDER BY {config["primary_key"]}
    """

    df = pd.read_sql(query, engine)

    output_dir = OUTPUT_DIR / table_name
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_file = (
        output_dir / f"{table_name}.parquet"
    )

    df.to_parquet(
        output_file,
        index=False,
    )

    file_size_mb = (
        output_file.stat().st_size
        / (1024 * 1024)
    )

    print(f"  Registros: {len(df):,}")
    print(f"  Colunas:   {len(df.columns)}")
    print(f"  Arquivo:   {output_file}")
    print(f"  Tamanho:   {file_size_mb:.2f} MB")


print("\nExtração concluída com sucesso.")