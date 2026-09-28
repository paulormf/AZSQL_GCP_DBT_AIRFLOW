select
    cast(id_pedido as int64) as id_pedido,
    cast(id_cliente as int64) as id_cliente,
    cast(id_vendedor as int64) as id_vendedor,
    cast(data_pedido as timestamp) as data_pedido,
    trim(status) as status,
    cast(valor_total as numeric) as valor_total,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from {{ source('bronze', 'pedidos') }}