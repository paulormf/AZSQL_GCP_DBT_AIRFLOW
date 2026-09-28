import os
import time
from datetime import datetime, timezone

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud import storage
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

from discover_tables import discover_tables
from config.extract_config import (
    TABLES_TO_EXTRACT,
    TABLE_CONFIG,
    FULL_RELOAD_TABLES,
)


# ============================================================
# CONFIGURAÇÃO
# ============================================================

load_dotenv()

GCP_PROJECT = "botbrewpub-ghsurp"

BRONZE_DATASET = "entrevistaporto_bronze"
STAGING_DATASET = "entrevistaporto_staging"
CONTROL_DATASET = "entrevistaporto_control"

WATERMARK_TABLE = "pipeline_watermark"

GCS_BUCKET = "entrevistaporto-data-lake"

LOCAL_FULL_DIR = "data/full"
LOCAL_INCREMENTAL_DIR = "data/incremental"


# ============================================================
# CONFIGURAÇÃO DE RETRY - AZURE SQL
# ============================================================

SQL_MAX_RETRIES = 3

SQL_RETRY_DELAYS = (
    5,
    15,
    30,
)


# ============================================================
# CLIENTES GCP
# ============================================================

bq_client = bigquery.Client(
    project=GCP_PROJECT
)

storage_client = storage.Client(
    project=GCP_PROJECT
)


# ============================================================
# CONEXÃO AZURE SQL
# ============================================================

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

    return create_engine(
        connection_url,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


sql_engine = create_sql_engine()


# ============================================================
# RETRY AZURE SQL
# ============================================================

def is_transient_sql_error(error):

    error_text = str(error)

    transient_errors = (
        "08001",
        "08S01",
        "HYT00",
        "HYT01",
        "connection reset",
        "connection refused",
        "connection is broken",
        "communication link failure",
        "timeout expired",
    )

    return any(
        error_code.lower() in error_text.lower()
        for error_code in transient_errors
    )


def execute_sql_with_retry(
    operation,
    description="operação SQL",
):

    last_error = None

    for attempt in range(
        1,
        SQL_MAX_RETRIES + 1,
    ):

        try:

            return operation()

        except OperationalError as error:

            last_error = error

            if not is_transient_sql_error(
                error
            ):

                raise

            if attempt >= SQL_MAX_RETRIES:

                print(
                    f"\nERRO: {description} "
                    f"falhou após "
                    f"{SQL_MAX_RETRIES} tentativas."
                )

                raise

            delay = SQL_RETRY_DELAYS[
                attempt - 1
            ]

            print(
                "\n" + "!" * 60
            )

            print(
                "FALHA TRANSITÓRIA AZURE SQL"
            )

            print(
                f"Operação: {description}"
            )

            print(
                f"Tentativa: "
                f"{attempt}/{SQL_MAX_RETRIES}"
            )

            print(
                f"Nova tentativa em "
                f"{delay} segundos..."
            )

            print(
                f"Erro: {error}"
            )

            print(
                "!" * 60
            )

            time.sleep(delay)

    raise last_error


# ============================================================
# IDs BIGQUERY
# ============================================================

def get_bronze_table_id(table_name):

    return (
        f"{GCP_PROJECT}."
        f"{BRONZE_DATASET}."
        f"{table_name}"
    )


def get_staging_table_id(table_name):

    return (
        f"{GCP_PROJECT}."
        f"{STAGING_DATASET}."
        f"{table_name}_incremental"
    )


def get_watermark_table_id():

    return (
        f"{GCP_PROJECT}."
        f"{CONTROL_DATASET}."
        f"{WATERMARK_TABLE}"
    )


# ============================================================
# VERIFICAR SE TABELA EXISTE NO BRONZE
# ============================================================

def bronze_exists(table_name):

    table_id = get_bronze_table_id(
        table_name
    )

    try:

        bq_client.get_table(
            table_id
        )

        return True

    except Exception:

        return False


# ============================================================
# WATERMARK
# ============================================================

def get_watermark(table_name):

    table_id = get_watermark_table_id()

    query = f"""
        SELECT
            table_name,
            watermark,
            watermark_source
        FROM `{table_id}`
        WHERE table_name = @table_name
        LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "table_name",
                "STRING",
                table_name,
            )
        ]
    )

    rows = list(
        bq_client.query(
            query,
            job_config=job_config,
        ).result()
    )

    if not rows:

        return None

    row = rows[0]

    if row.watermark_source:

        return row.watermark_source

    if row.watermark:

        return (
            row.watermark.strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )
            + "0"
        )

    return None


def update_watermark(
    table_name,
    watermark_source,
):

    table_id = get_watermark_table_id()

    query = f"""
        MERGE `{table_id}` AS target

        USING (
            SELECT
                @table_name AS table_name,
                TIMESTAMP(
                    SUBSTR(
                        @watermark_source,
                        1,
                        26
                    )
                ) AS watermark,
                @watermark_source AS watermark_source
        ) AS source

        ON target.table_name = source.table_name

        WHEN MATCHED THEN
            UPDATE SET
                watermark = source.watermark,
                watermark_source = source.watermark_source

        WHEN NOT MATCHED THEN
            INSERT (
                table_name,
                watermark,
                watermark_source
            )
            VALUES (
                source.table_name,
                source.watermark,
                source.watermark_source
            )
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "table_name",
                "STRING",
                table_name,
            ),
            bigquery.ScalarQueryParameter(
                "watermark_source",
                "STRING",
                watermark_source,
            ),
        ]
    )

    bq_client.query(
        query,
        job_config=job_config,
    ).result()


# ============================================================
# MAX UPDATED_AT DA ORIGEM
# ============================================================

def get_source_max_updated_at(
    table_name,
    metadata,
    watermark=None,
):

    source_table = metadata[
        "source_table"
    ]

    if watermark:

        sql = text(f"""
            SELECT
                CONVERT(
                    VARCHAR(27),
                    MAX([updated_at]),
                    126
                ) AS max_updated_at
            FROM {source_table}
            WHERE [updated_at] >
                  CONVERT(
                      DATETIME2(7),
                      :watermark
                  )
        """)

        params = {
            "watermark": watermark,
        }

    else:

        sql = text(f"""
            SELECT
                CONVERT(
                    VARCHAR(27),
                    MAX([updated_at]),
                    126
                ) AS max_updated_at
            FROM {source_table}
        """)

        params = {}

    def execute():

        with sql_engine.connect() as connection:

            return connection.execute(
                sql,
                params,
            ).fetchone()

    result = execute_sql_with_retry(
        execute,
        description=(
            f"MAX(updated_at) - {table_name}"
        ),
    )

    if (
        not result
        or result.max_updated_at is None
    ):

        return None

    return result.max_updated_at


# ============================================================
# FULL LOAD - DESCOBRIR FAIXA DE ID
# ============================================================

def get_id_range(
    table_name,
    metadata,
    config,
):

    source_table = metadata[
        "source_table"
    ]

    partition_column = config[
        "partition_column"
    ]

    sql = text(f"""
        SELECT
            MIN([{partition_column}])
                AS min_value,

            MAX([{partition_column}])
                AS max_value

        FROM {source_table}
    """)

    def execute():

        with sql_engine.connect() as connection:

            return connection.execute(
                sql
            ).fetchone()

    result = execute_sql_with_retry(
        execute,
        description=(
            f"faixa de ID - {table_name}"
        ),
    )

    if not result:

        return None, None

    if result.min_value is None:

        return None, None

    return (
        result.min_value,
        result.max_value,
    )


# ============================================================
# FULL LOAD - GERAR FAIXAS DE ID
# ============================================================

def generate_id_ranges(
    min_value,
    max_value,
    batch_size,
):

    current = min_value

    while current <= max_value:

        next_value = (
            current + batch_size
        )

        yield (
            current,
            next_value,
        )

        current = next_value


# ============================================================
# FULL LOAD - EXTRAIR LOTE POR ID
# ============================================================

def extract_full_batch(
    table_name,
    metadata,
    config,
    start_value,
    end_value,
):

    source_table = metadata[
        "source_table"
    ]

    columns = metadata[
        "columns"
    ]

    partition_column = config[
        "partition_column"
    ]

    select_columns = ", ".join(
        f"[{column}]"
        for column in columns
    )

    sql = text(f"""
        SELECT
            {select_columns}

        FROM {source_table}

        WHERE [{partition_column}]
              >= :start_value

          AND [{partition_column}]
              < :end_value

        ORDER BY [{partition_column}]
    """)

    print(
        f"Extraindo {source_table}: "
        f"{partition_column} >= {start_value} "
        f"AND {partition_column} < {end_value}"
    )

    def execute():

        return pd.read_sql(
            sql,
            sql_engine,
            params={
                "start_value": start_value,
                "end_value": end_value,
            },
        )

    return execute_sql_with_retry(
        execute,
        description=(
            f"extração FULL - {table_name} "
            f"lote {start_value}-{end_value}"
        ),
    )


# ============================================================
# FULL LOAD - DESCOBRIR FAIXA DE DATA
# ============================================================

def get_date_range(
    table_name,
    metadata,
    config,
):

    source_table = metadata[
        "source_table"
    ]

    partition_column = config[
        "partition_column"
    ]

    sql = text(f"""
        SELECT
            MIN([{partition_column}])
                AS min_value,

            MAX([{partition_column}])
                AS max_value,

            SUM(
                CASE
                    WHEN [{partition_column}]
                         IS NULL
                    THEN 1
                    ELSE 0
                END
            ) AS null_count

        FROM {source_table}
    """)

    def execute():

        with sql_engine.connect() as connection:

            return connection.execute(
                sql
            ).fetchone()

    result = execute_sql_with_retry(
        execute,
        description=(
            f"faixa de data - {table_name}"
        ),
    )

    if not result:

        return None, None, 0

    return (
        result.min_value,
        result.max_value,
        result.null_count or 0,
    )


# ============================================================
# FULL LOAD - GERAR INTERVALOS MENSAIS
# ============================================================

def generate_month_ranges(
    min_value,
    max_value,
):

    current = min_value.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    last_month = max_value.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    while current <= last_month:

        if current.month == 12:

            next_month = current.replace(
                year=current.year + 1,
                month=1,
            )

        else:

            next_month = current.replace(
                month=current.month + 1,
            )

        yield (
            current,
            next_month,
        )

        current = next_month


# ============================================================
# FULL LOAD - EXTRAIR LOTE POR DATA
# ============================================================

def extract_full_date_batch(
    table_name,
    metadata,
    config,
    start_date,
    end_date,
):

    source_table = metadata[
        "source_table"
    ]

    columns = metadata[
        "columns"
    ]

    partition_column = config[
        "partition_column"
    ]

    select_columns = ", ".join(
        f"[{column}]"
        for column in columns
    )

    sql = text(f"""
        SELECT
            {select_columns}

        FROM {source_table}

        WHERE [{partition_column}]
              >= :start_date

          AND [{partition_column}]
              < :end_date

        ORDER BY [{partition_column}]
    """)

    print(
        f"Extraindo {source_table}: "
        f"{partition_column} >= {start_date} "
        f"AND {partition_column} < {end_date}"
    )

    def execute():

        return pd.read_sql(
            sql,
            sql_engine,
            params={
                "start_date": start_date,
                "end_date": end_date,
            },
        )

    return execute_sql_with_retry(
        execute,
        description=(
            f"extração FULL por data - "
            f"{table_name} "
            f"{start_date} -> {end_date}"
        ),
    )


# ============================================================
# FULL LOAD - EXTRAIR REGISTROS COM DATA NULL
# ============================================================

def extract_full_date_null_batch(
    table_name,
    metadata,
    config,
):

    source_table = metadata[
        "source_table"
    ]

    columns = metadata[
        "columns"
    ]

    partition_column = config[
        "partition_column"
    ]

    select_columns = ", ".join(
        f"[{column}]"
        for column in columns
    )

    sql = text(f"""
        SELECT
            {select_columns}

        FROM {source_table}

        WHERE [{partition_column}]
              IS NULL
    """)

    print(
        f"Extraindo registros com "
        f"{partition_column} IS NULL"
    )

    def execute():

        return pd.read_sql(
            sql,
            sql_engine,
        )

    return execute_sql_with_retry(
        execute,
        description=(
            f"extração NULL - {table_name}"
        ),
    )


# ============================================================
# FULL LOAD - EXTRAÇÃO COMPLETA
# ============================================================

def extract_full(
    table_name,
    metadata,
):

    source_table = metadata[
        "source_table"
    ]

    columns = metadata[
        "columns"
    ]

    select_columns = ", ".join(
        f"[{column}]"
        for column in columns
    )

    sql = text(f"""
        SELECT
            {select_columns}

        FROM {source_table}
    """)

    print(
        f"Extraindo FULL: "
        f"{source_table}"
    )

    def execute():

        return pd.read_sql(
            sql,
            sql_engine,
        )

    return execute_sql_with_retry(
        execute,
        description=(
            f"extração FULL - {table_name}"
        ),
    )


# ============================================================
# INCREMENTAL - EXTRAIR ALTERAÇÕES
# ============================================================

def extract_incremental(
    table_name,
    metadata,
    watermark,
):

    source_table = metadata[
        "source_table"
    ]

    columns = metadata[
        "columns"
    ]

    select_columns = ", ".join(
        f"[{column}]"
        for column in columns
    )

    sql = text(f"""
        SELECT
            {select_columns}

        FROM {source_table}

        WHERE [updated_at] >
              CONVERT(
                  DATETIME2(7),
                  :watermark
              )

        ORDER BY [updated_at]
    """)

    print(
        f"\nConsultando: "
        f"{source_table}"
    )

    print(
        f"Watermark: {watermark}"
    )

    def execute():

        return pd.read_sql(
            sql,
            sql_engine,
            params={
                "watermark": watermark,
            },
        )

    return execute_sql_with_retry(
        execute,
        description=(
            f"extração incremental - "
            f"{table_name}"
        ),
    )


# ============================================================
# SALVAR PARQUET
# ============================================================

def save_parquet(
    df,
    table_name,
    mode,
    batch_number=None,
):

    now = datetime.now(
        timezone.utc
    )

    timestamp = now.strftime(
        "%Y%m%dT%H%M%SZ"
    )

    if mode == "full":

        directory = os.path.join(
            LOCAL_FULL_DIR,
            table_name,
        )

        os.makedirs(
            directory,
            exist_ok=True,
        )

        if batch_number is not None:

            filename = (
                f"{table_name}_part_"
                f"{batch_number:05d}.parquet"
            )

        else:

            filename = (
                f"{table_name}.parquet"
            )

    else:

        directory = os.path.join(
            LOCAL_INCREMENTAL_DIR,
            table_name,
        )

        os.makedirs(
            directory,
            exist_ok=True,
        )

        filename = (
            f"{table_name}_"
            f"{timestamp}.parquet"
        )

    file_path = os.path.join(
        directory,
        filename
    )

    df.to_parquet(
        file_path,
        index=False,
    )

    return (
        file_path,
        timestamp,
    )


# ============================================================
# UPLOAD GCS
# ============================================================

def upload_to_gcs(
    file_path,
    table_name,
    mode,
    timestamp,
    batch_number=None,
):

    bucket = storage_client.bucket(
        GCS_BUCKET
    )

    filename = os.path.basename(
        file_path
    )

    if mode == "full":

        blob_name = (
            f"raw/{table_name}/full/"
            f"{filename}"
        )

    else:

        date = datetime.now(
            timezone.utc
        )

        blob_name = (
            f"raw/incremental/"
            f"{table_name}/"
            f"{date:%Y/%m/%d}/"
            f"{filename}"
        )

    blob = bucket.blob(
        blob_name
    )

    blob.upload_from_filename(
        file_path
    )

    return (
        f"gs://{GCS_BUCKET}/"
        f"{blob_name}"
    )


# ============================================================
# LOAD PARQUET NO BIGQUERY
# ============================================================

def load_parquet_to_bigquery(
    gcs_uri,
    table_name,
    write_disposition,
):

    table_id = get_bronze_table_id(
        table_name
    )

    job_config = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.PARQUET
        ),
        write_disposition=(
            write_disposition
        ),
    )

    job = bq_client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config,
    )

    job.result()

    return bq_client.get_table(
        table_id
    )


# ============================================================
# LOAD STAGING
# ============================================================

def load_incremental_staging(
    gcs_uri,
    table_name,
):

    staging_table_id = (
        get_staging_table_id(
            table_name
        )
    )

    job_config = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.PARQUET
        ),
        write_disposition=(
            bigquery.WriteDisposition
            .WRITE_TRUNCATE
        ),
    )

    job = bq_client.load_table_from_uri(
        gcs_uri,
        staging_table_id,
        job_config=job_config,
    )

    job.result()

    return bq_client.get_table(
        staging_table_id
    )


# ============================================================
# MERGE INCREMENTAL NO BRONZE
# ============================================================

def merge_incremental(
    table_name,
    metadata,
):

    staging_table_id = (
        get_staging_table_id(
            table_name
        )
    )

    bronze_table_id = (
        get_bronze_table_id(
            table_name
        )
    )

    primary_key = metadata[
        "primary_key"
    ]

    columns = metadata[
        "columns"
    ]

    update_columns = [
        column
        for column in columns
        if column != primary_key
    ]

    update_clause = ",\n".join(
        f"target.`{column}` = "
        f"source.`{column}`"
        for column in update_columns
    )

    insert_columns = ", ".join(
        f"`{column}`"
        for column in columns
    )

    insert_values = ", ".join(
        f"source.`{column}`"
        for column in columns
    )

    query = f"""
        MERGE `{bronze_table_id}` AS target

        USING `{staging_table_id}` AS source

        ON target.`{primary_key}`
           = source.`{primary_key}`

        WHEN MATCHED THEN
            UPDATE SET
                {update_clause}

        WHEN NOT MATCHED THEN
            INSERT (
                {insert_columns}
            )
            VALUES (
                {insert_values}
            )
    """

    job = bq_client.query(
        query
    )

    job.result()


# ============================================================
# PROCESSAR FULL POR ID
# ============================================================

def process_full_by_id(
    table_name,
    metadata,
    config,
):

    print(
        f"Executando FULL LOAD particionado "
        f"por ID: {table_name}"
    )

    partition_column = config[
        "partition_column"
    ]

    batch_size = config[
        "batch_size"
    ]

    print(
        f"Coluna de particionamento: "
        f"{partition_column}"
    )

    print(
        f"Tamanho do lote: "
        f"{batch_size}"
    )

    min_value, max_value = get_id_range(
        table_name,
        metadata,
        config,
    )

    if min_value is None:

        print(
            "Tabela sem registros."
        )

        return

    print(
        f"Faixa encontrada: "
        f"{min_value} → {max_value}"
    )

    total_records = 0
    batch_number = 0
    gcs_uris = []

    for start_value, end_value in (
        generate_id_ranges(
            min_value,
            max_value,
            batch_size,
        )
    ):

        batch_number += 1

        print(
            "\n" + "-" * 60
        )

        print(
            f"LOTE {batch_number}"
        )

        print(
            f"Faixa: {start_value} → "
            f"{end_value - 1}"
        )

        df = extract_full_batch(
            table_name,
            metadata,
            config,
            start_value,
            end_value,
        )

        if df.empty:

            print(
                "Nenhum registro neste lote."
            )

            continue

        records = len(df)

        total_records += records

        print(
            f"Registros extraídos: "
            f"{records}"
        )

        file_path, timestamp = (
            save_parquet(
                df,
                table_name,
                "full",
                batch_number,
            )
        )

        print(
            f"Parquet: {file_path}"
        )

        gcs_uri = upload_to_gcs(
            file_path,
            table_name,
            "full",
            timestamp,
            batch_number,
        )

        print(
            f"GCS: {gcs_uri}"
        )

        gcs_uris.append(
            gcs_uri
        )

    if not gcs_uris:

        print(
            "Nenhum arquivo foi gerado."
        )

        return

    print(
        "\n" + "=" * 60
    )

    print(
        "CARREGANDO FULL NO BIGQUERY"
    )

    print(
        "=" * 60
    )

    table_id = get_bronze_table_id(
        table_name
    )

    job_config = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.PARQUET
        ),
        write_disposition=(
            bigquery.WriteDisposition
            .WRITE_TRUNCATE
        ),
    )

    print(
        f"Tabela destino: {table_id}"
    )

    print(
        f"Arquivos GCS: "
        f"{len(gcs_uris)}"
    )

    job = bq_client.load_table_from_uri(
        gcs_uris,
        table_id,
        job_config=job_config,
    )

    job.result()

    table = bq_client.get_table(
        table_id
    )

    print(
        f"BigQuery carregado: "
        f"{table.num_rows} registros"
    )

    source_max_updated_at = (
        get_source_max_updated_at(
            table_name,
            metadata,
        )
    )

    if source_max_updated_at is None:

        raise ValueError(
            "Não foi possível determinar "
            "o watermark inicial."
        )

    update_watermark(
        table_name,
        source_max_updated_at,
    )

    print(
        f"Watermark inicial: "
        f"{source_max_updated_at}"
    )

    print(
        "\n" + "=" * 60
    )

    print(
        f"FULL LOAD CONCLUÍDO: "
        f"{table_name}"
    )

    print(
        f"Lotes processados: "
        f"{batch_number}"
    )

    print(
        f"Registros extraídos: "
        f"{total_records}"
    )

    print(
        f"Registros no BigQuery: "
        f"{table.num_rows}"
    )

    print(
        "=" * 60
    )


# ============================================================
# PROCESSAR FULL POR DATA
# ============================================================

def process_full_by_date(
    table_name,
    metadata,
    config,
):

    print(
        f"Executando FULL LOAD particionado "
        f"por DATA: {table_name}"
    )

    partition_column = config[
        "partition_column"
    ]

    print(
        f"Coluna de particionamento: "
        f"{partition_column}"
    )

    min_value, max_value, null_count = (
        get_date_range(
            table_name,
            metadata,
            config,
        )
    )

    if (
        min_value is None
        and null_count == 0
    ):

        print(
            "Tabela sem registros."
        )

        return

    if min_value is not None:

        print(
            f"Faixa encontrada: "
            f"{min_value} → {max_value}"
        )

    print(
        f"Registros com data NULL: "
        f"{null_count}"
    )

    total_records = 0
    batch_number = 0
    gcs_uris = []

    # --------------------------------------------------------
    # LOTES MENSAIS
    # --------------------------------------------------------

    if min_value is not None:

        for start_date, end_date in (
            generate_month_ranges(
                min_value,
                max_value,
            )
        ):

            batch_number += 1

            print(
                "\n" + "-" * 60
            )

            print(
                f"LOTE {batch_number}"
            )

            print(
                f"Período: "
                f"{start_date} → {end_date}"
            )

            df = extract_full_date_batch(
                table_name,
                metadata,
                config,
                start_date,
                end_date,
            )

            if df.empty:

                print(
                    "Nenhum registro neste período."
                )

                continue

            records = len(df)

            total_records += records

            print(
                f"Registros extraídos: "
                f"{records}"
            )

            file_path, timestamp = (
                save_parquet(
                    df,
                    table_name,
                    "full",
                    batch_number,
                )
            )

            print(
                f"Parquet: {file_path}"
            )

            gcs_uri = upload_to_gcs(
                file_path,
                table_name,
                "full",
                timestamp,
                batch_number,
            )

            print(
                f"GCS: {gcs_uri}"
            )

            gcs_uris.append(
                gcs_uri
            )

    # --------------------------------------------------------
    # REGISTROS COM DATA NULL
    # --------------------------------------------------------

    if null_count > 0:

        batch_number += 1

        print(
            "\n" + "-" * 60
        )

        print(
            f"LOTE {batch_number}"
        )

        print(
            f"Registros com "
            f"{partition_column} IS NULL"
        )

        df = extract_full_date_null_batch(
            table_name,
            metadata,
            config,
        )

        if not df.empty:

            records = len(df)

            total_records += records

            print(
                f"Registros extraídos: "
                f"{records}"
            )

            file_path, timestamp = (
                save_parquet(
                    df,
                    table_name,
                    "full",
                    batch_number,
                )
            )

            print(
                f"Parquet: {file_path}"
            )

            gcs_uri = upload_to_gcs(
                file_path,
                table_name,
                "full",
                timestamp,
                batch_number,
            )

            print(
                f"GCS: {gcs_uri}"
            )

            gcs_uris.append(
                gcs_uri
            )

    # --------------------------------------------------------
    # VALIDAR ARQUIVOS
    # --------------------------------------------------------

    if not gcs_uris:

        print(
            "Nenhum arquivo foi gerado."
        )

        return

    # --------------------------------------------------------
    # LOAD NO BIGQUERY
    # --------------------------------------------------------

    print(
        "\n" + "=" * 60
    )

    print(
        "CARREGANDO FULL NO BIGQUERY"
    )

    print(
        "=" * 60
    )

    table_id = get_bronze_table_id(
        table_name
    )

    job_config = bigquery.LoadJobConfig(
        source_format=(
            bigquery.SourceFormat.PARQUET
        ),
        write_disposition=(
            bigquery.WriteDisposition
            .WRITE_TRUNCATE
        ),
    )

    print(
        f"Tabela destino: {table_id}"
    )

    print(
        f"Arquivos GCS: "
        f"{len(gcs_uris)}"
    )

    job = bq_client.load_table_from_uri(
        gcs_uris,
        table_id,
        job_config=job_config,
    )

    job.result()

    table = bq_client.get_table(
        table_id
    )

    print(
        f"BigQuery carregado: "
        f"{table.num_rows} registros"
    )

    # --------------------------------------------------------
    # WATERMARK
    # --------------------------------------------------------

    source_max_updated_at = (
        get_source_max_updated_at(
            table_name,
            metadata,
        )
    )

    if source_max_updated_at is None:

        raise ValueError(
            "Não foi possível determinar "
            "o watermark inicial."
        )

    update_watermark(
        table_name,
        source_max_updated_at,
    )

    print(
        f"Watermark inicial: "
        f"{source_max_updated_at}"
    )

    # --------------------------------------------------------
    # RESUMO
    # --------------------------------------------------------

    print(
        "\n" + "=" * 60
    )

    print(
        f"FULL LOAD CONCLUÍDO: "
        f"{table_name}"
    )

    print(
        f"Lotes processados: "
        f"{batch_number}"
    )

    print(
        f"Registros extraídos: "
        f"{total_records}"
    )

    print(
        f"Registros no BigQuery: "
        f"{table.num_rows}"
    )

    print(
        "=" * 60
    )


# ============================================================
# PROCESSAR FULL GENÉRICO
# ============================================================

def process_full(
    table_name,
    metadata,
):

    df = extract_full(
        table_name,
        metadata,
    )

    if df.empty:

        print(
            "Tabela sem registros."
        )

        return

    print(
        f"Registros extraídos: "
        f"{len(df)}"
    )

    file_path, timestamp = (
        save_parquet(
            df,
            table_name,
            "full",
        )
    )

    print(
        f"Parquet: {file_path}"
    )

    gcs_uri = upload_to_gcs(
        file_path,
        table_name,
        "full",
        timestamp,
    )

    print(
        f"GCS: {gcs_uri}"
    )


# ============================================================
# PROCESSAR INCREMENTAL
# ============================================================

def process_incremental(
    table_name,
    metadata,
    watermark,
):

    df = extract_incremental(
        table_name,
        metadata,
        watermark,
    )

    if df.empty:

        print(
            "Nenhum registro novo "
            "ou alterado."
        )

        return

    print(
        f"Registros encontrados: "
        f"{len(df)}"
    )

    new_watermark = (
        get_source_max_updated_at(
            table_name,
            metadata,
            watermark,
        )
    )

    if new_watermark is None:

        raise ValueError(
            "Não foi possível determinar "
            "o novo watermark."
        )

    print(
        f"Novo watermark origem: "
        f"{new_watermark}"
    )

    file_path, timestamp = (
        save_parquet(
            df,
            table_name,
            "incremental",
        )
    )

    print(
        f"Parquet criado: "
        f"{file_path}"
    )

    gcs_uri = upload_to_gcs(
        file_path,
        table_name,
        "incremental",
        timestamp,
    )

    print(
        f"GCS: {gcs_uri}"
    )

    load_incremental_staging(
        gcs_uri,
        table_name,
    )

    print(
        "Staging carregada: "
        f"{get_staging_table_id(table_name)}"
    )

    merge_incremental(
        table_name,
        metadata,
    )

    print(
        "MERGE Bronze concluído."
    )

    update_watermark(
        table_name,
        new_watermark,
    )

    print(
        f"Watermark atualizado: "
        f"{new_watermark}"
    )

    print(
        f"SUCESSO: {table_name}"
    )


# ============================================================
# PROCESSAR TABELA
# ============================================================

def process_table(
    table_name,
    metadata,
    force_full=False,
):

    print(
        "\n" + "=" * 70
    )

    print(
        f"PROCESSANDO: "
        f"{table_name}"
    )

    print(
        "=" * 70
    )

    watermark = get_watermark(
        table_name
    )

    exists = bronze_exists(
        table_name
    )

    # --------------------------------------------------------
    # FULL RELOAD FORÇADO
    # --------------------------------------------------------

    if force_full:

        print(
            "\nFULL RELOAD solicitado "
            f"para {table_name}."
        )

        config = TABLE_CONFIG.get(
            table_name
        )

        if not config:

            raise ValueError(
                f"Não existe configuração "
                f"para {table_name}"
            )

        partition_type = config[
            "partition_type"
        ]

        if partition_type == "id":

            process_full_by_id(
                table_name,
                metadata,
                config,
            )

            return

        elif partition_type == "date":

            process_full_by_date(
                table_name,
                metadata,
                config,
            )

            return

        else:

            raise ValueError(
                f"Tipo de particionamento "
                f"não suportado: "
                f"{partition_type}"
            )

    # --------------------------------------------------------
    # FULL LOAD AUTOMÁTICO
    # --------------------------------------------------------

    if not exists:

        print(
            "\nTabela ainda não existe "
            "no Bronze."
        )

        config = TABLE_CONFIG.get(
            table_name
        )

        if not config:

            raise ValueError(
                f"Não existe configuração "
                f"para {table_name}"
            )

        partition_type = config[
            "partition_type"
        ]

        if partition_type == "id":

            process_full_by_id(
                table_name,
                metadata,
                config,
            )

            return

        elif partition_type == "date":

            process_full_by_date(
                table_name,
                metadata,
                config,
            )

            return

        else:

            raise ValueError(
                f"Tipo de particionamento "
                f"não suportado: "
                f"{partition_type}"
            )

    # --------------------------------------------------------
    # INCREMENTAL
    # --------------------------------------------------------

    if not watermark:

        raise ValueError(
            f"Tabela {table_name} "
            f"existe no Bronze, mas "
            f"não possui watermark."
        )

    process_incremental(
        table_name,
        metadata,
        watermark,
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print("\n")

    print(
        "=" * 70
    )

    print(
        "PIPELINE DE INGESTÃO"
    )

    print(
        "=" * 70
    )

    # --------------------------------------------------------
    # DESCOBRIR TABELAS NO SQL SERVER
    # --------------------------------------------------------

    tables = discover_tables(
        sql_engine,
        schema="dbo",
    )

    # --------------------------------------------------------
    # VALIDAR TABELAS
    # --------------------------------------------------------

    for table_name in TABLES_TO_EXTRACT:

        if table_name not in tables:

            raise ValueError(
                f"Tabela {table_name} "
                f"não encontrada "
                f"no SQL Server."
            )

        metadata = tables[
            table_name
        ]

        if not metadata[
            "primary_key"
        ]:

            raise ValueError(
                f"Tabela {table_name} "
                f"não possui chave primária."
            )

        if "updated_at" not in metadata[
            "columns"
        ]:

            raise ValueError(
                f"Tabela {table_name} "
                f"não possui coluna "
                f"updated_at."
            )

    # --------------------------------------------------------
    # CLASSIFICAR FULL E INCREMENTAL
    # --------------------------------------------------------

    incremental_tables = []

    full_tables = []

    for table_name in TABLES_TO_EXTRACT:

        # ----------------------------------------------------
        # FULL RELOAD EXPLÍCITO
        # ----------------------------------------------------

        if table_name in FULL_RELOAD_TABLES:

            full_tables.append(
                table_name
            )

        # ----------------------------------------------------
        # FULL AUTOMÁTICO
        # ----------------------------------------------------

        elif not bronze_exists(
            table_name
        ):

            full_tables.append(
                table_name
            )

        # ----------------------------------------------------
        # INCREMENTAL
        # ----------------------------------------------------

        else:

            incremental_tables.append(
                table_name
            )

    # --------------------------------------------------------
    # MOSTRAR ORDEM
    # --------------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "ORDEM DE EXECUÇÃO"
    )

    print(
        "=" * 70
    )

    print(
        "\nINCREMENTAL:"
    )

    for table_name in incremental_tables:

        print(
            f"  - {table_name}"
        )

    print(
        "\nFULL:"
    )

    for table_name in full_tables:

        print(
            f"  - {table_name}"
        )

    # --------------------------------------------------------
    # EXECUTAR INCREMENTAIS PRIMEIRO
    # --------------------------------------------------------

    for table_name in incremental_tables:

        process_table(
            table_name,
            tables[table_name],
            force_full=False,
        )

    # --------------------------------------------------------
    # EXECUTAR FULL DEPOIS
    # --------------------------------------------------------

    for table_name in full_tables:

        process_table(
            table_name,
            tables[table_name],
            force_full=True,
        )


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":

    main()
