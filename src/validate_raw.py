from pathlib import Path

import pandas as pd


tables = [
    "clientes",
    "vendedores",
    "produtos",
    "pedidos",
    "itens_pedido",
    "pagamentos",
]


base_path = Path("data/raw")

total = 0

print("\nVALIDAÇÃO DOS PARQUETS")
print("=" * 60)

for table in tables:
    file_path = base_path / table / f"{table}.parquet"

    df = pd.read_parquet(file_path)

    quantidade = len(df)
    total += quantidade

    print(f"{table:<20} {quantidade:>10,} registros")

print("=" * 60)
print(f"{'TOTAL':<20} {total:>10,} registros")