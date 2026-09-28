from google.cloud import bigquery

from config.extract_config import TABLES_TO_EXTRACT


PROJECT_ID = "botbrewpub-ghsurp"
DATASET_ID = "entrevistaporto_bronze"
BUCKET_NAME = "entrevistaporto-data-lake"


client = bigquery.Client(project=PROJECT_ID)

success = []
failed = []


print("\nLOAD GCS → BIGQUERY BRONZE")
print("=" * 60)


for table_name in TABLES_TO_EXTRACT:

    source_uri = (
        f"gs://{BUCKET_NAME}/"
        f"raw/{table_name}/{table_name}.parquet"
    )

    destination = (
        f"{PROJECT_ID}."
        f"{DATASET_ID}."
        f"{table_name}"
    )

    print(f"\nCarregando: {table_name}")
    print(f"  Origem:    {source_uri}")
    print(f"  Destino:   {destination}")

    try:

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
        )

        load_job = client.load_table_from_uri(
            source_uri,
            destination,
            job_config=job_config,
        )

        load_job.result()

        table = client.get_table(destination)

        print(f"  Registros: {table.num_rows:,}")
        print(f"  Colunas:   {len(table.schema)}")
        print("  Status:    OK")

        success.append(table_name)

    except Exception as error:

        print(f"  ERRO: {error}")
        failed.append(table_name)


print("\n" + "=" * 60)
print("RESUMO DO LOAD")
print("=" * 60)

print(f"Sucesso: {len(success)}")
print(f"Falhas:  {len(failed)}")


if success:

    print("\nTabelas carregadas:")

    for table in success:
        print(f"  ✓ {table}")


if failed:

    print("\nTabelas com erro:")

    for table in failed:
        print(f"  ✗ {table}")

    raise SystemExit(1)


print("\nCarga Bronze concluída com sucesso.")