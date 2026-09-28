TABLES_TO_EXTRACT = [
    "clientes",
    "vendedores",
    "produtos",
    "pedidos",
    "itens_pedido",
    "pagamentos",
    "avaliacoes_clientes",
]

FULL_RELOAD_TABLES = {
    
}


TABLE_CONFIG = {
    "clientes": {
        "partition_column": "id_cliente",
        "partition_type": "id",
        "batch_size": 2000,
    },

    "vendedores": {
        "partition_column": "id_vendedor",
        "partition_type": "id",
        "batch_size": 200,
    },

    "produtos": {
        "partition_column": "id_produto",
        "partition_type": "id",
        "batch_size": 1000,
    },

    "pedidos": {
        "partition_column": "data_pedido",
        "partition_type": "date",
        "partition_interval": "month",
    },

    "itens_pedido": {
        "partition_column": "id_item",
        "partition_type": "id",
        "batch_size": 10000,
    },

    "pagamentos": {
        "partition_column": "data_pagamento",
        "partition_type": "date",
        "partition_interval": "month",
    },

    "avaliacoes_clientes": {
        "partition_column": "id_avaliacao",
        "partition_type": "id",
        "batch_size": 1000,
    },    
}