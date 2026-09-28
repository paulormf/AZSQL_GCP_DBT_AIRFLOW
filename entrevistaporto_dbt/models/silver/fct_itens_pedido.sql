{{ config(
    materialized='table',
    partition_by={
        "field": "created_at",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "id_pedido",
        "id_produto"
    ]
) }}

select
    id_item,
    id_pedido,
    id_produto,

    quantidade,
    preco_unitario,
    desconto,

    quantidade * preco_unitario as valor_bruto,

    quantidade * preco_unitario * (desconto / 100) as valor_desconto,

    quantidade * preco_unitario
        - (quantidade * preco_unitario * (desconto / 100))
        as valor_liquido,

    created_at,
    updated_at

from {{ ref('stg_itens_pedido') }}