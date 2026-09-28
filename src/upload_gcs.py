from pathlib import Path

from google.cloud import storage

from config.extract_config import TABLES_TO_EXTRACT

PROJECT_ID = "botbrewpub-ghsurp"
BUCKET_NAME = "entrevistaporto-data-lake"

SOURCE_DIR = Path("data/raw")

client = storage.Client(project=PROJECT_ID)
bucket = client.bucket(BUCKET_NAME)

success = []
failed = []

print("\nUPLOAD PARA GOOGLE CLOUD STORAGE")
print("=" * 60)

for table_name in TABLES_TO_EXTRACT:

    local_file = (
        SOURCE_DIR
        / table_name
        / f"{table_name}.parquet"
    )

    blob_name = (
        f"raw/{table_name}/"
        f"{table_name}.parquet"
    )

    print(f"\nEnviando: {table_name}")

    if not local_file.exists():
        print(
            f"  ERRO: arquivo não encontrado: "
            f"{local_file}"
        )
        failed.append(table_name)
        continue

    try:
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(
            str(local_file)
        )

        print(f"  GCS: gs://{BUCKET_NAME}/{blob_name}")
        print("  Upload concluído.")

        success.append(table_name)

    except Exception as error:
        print(f"  ERRO no upload: {error}")
        failed.append(table_name)

print("\n" + "=" * 60)
print("RESUMO DO UPLOAD")
print("=" * 60)

print(f"Sucesso: {len(success)}")
print(f"Falhas:  {len(failed)}")

if success:
    print("\nTabelas enviadas:")

    for table in success:
        print(f"  ✓ {table}")

if failed:
    print("\nTabelas com erro:")

    for table in failed:
        print(f"  ✗ {table}")

    raise SystemExit(1)

print("\nUpload concluído com sucesso.")