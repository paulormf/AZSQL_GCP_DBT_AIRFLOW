select
    cast(id_item as int64) as id_item,
    cast(id_pedido as int64) as id_pedido,
    cast(id_produto as int64) as id_produto,

    cast(quantidade as numeric) as quantidade,
    cast(preco_unitario as numeric) as preco_unitario,
    cast(desconto as numeric) as desconto,

    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from {{ source('bronze', 'itens_pedido') }}