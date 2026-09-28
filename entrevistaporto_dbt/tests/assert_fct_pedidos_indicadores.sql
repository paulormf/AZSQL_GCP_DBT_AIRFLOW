select
    id_pedido,
    pedido_faturado,
    pedido_cancelado
from {{ ref('fct_pedidos') }}
where pedido_faturado not in (0, 1)
   or pedido_cancelado not in (0, 1)