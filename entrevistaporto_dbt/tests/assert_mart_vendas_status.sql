select
    id_pedido,
    status,
    pedido_faturado,
    pedido_cancelado,
    valor_total,
    valor_faturado

from {{ ref('mart_vendas') }}

where
    (status = 'FATURADO' and pedido_faturado != 1)
    or
    (status != 'FATURADO' and pedido_faturado != 0)
    or
    (status = 'CANCELADO' and pedido_cancelado != 1)
    or
    (status != 'CANCELADO' and pedido_cancelado != 0)