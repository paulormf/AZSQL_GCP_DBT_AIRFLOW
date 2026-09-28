import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL


load_dotenv()


def create_sql_engine():
    connection_url = URL.create(
        "mssql+pyodbc",
        username=os.getenv("AZURE_SQL_USERNAME"),
        password=os.getenv("AZURE_SQL_PASSWORD"),
        host=os.getenv("AZURE_SQL_SERVER"),
        database=os.getenv("AZURE_SQL_DATABASE"),
        query={
            "driver": "ODBC Driver 18 for SQL Server",
            "Encrypt": "yes",
            "TrustServerCertificate": "no",
        },
    )

    return create_engine(connection_url)


def discover_tables(engine, schema="dbo"):
    inspector = inspect(engine)

    table_names = inspector.get_table_names(
        schema=schema
    )

    tables = {}

    for table_name in table_names:

        columns_info = inspector.get_columns(
            table_name,
            schema=schema,
        )

        primary_key_info = inspector.get_pk_constraint(
            table_name,
            schema=schema,
        )

        primary_key_columns = primary_key_info.get(
            "constrained_columns",
            [],
        )

        columns = [
            column["name"]
            for column in columns_info
        ]

        tables[table_name] = {
            "source_table": f"{schema}.{table_name}",
            "primary_key": (
                primary_key_columns[0]
                if primary_key_columns
                else None
            ),
            "columns": columns,
        }

    return tables


if __name__ == "__main__":

    engine = create_sql_engine()

    tables = discover_tables(engine)

    print(f"\nTabelas encontradas: {len(tables)}")

    for table_name, config in sorted(tables.items()):

        print(
            f"  {table_name:<20} "
            f"{len(config['columns']):>3} colunas | "
            f"PK: {config['primary_key']}"
        )