select
    i.id_item,
    i.id_pedido,

    p.data_pedido,
    p.status,

    i.id_produto,
    pr.nome as nome_produto,
    pr.categoria,
    pr.marca,

    p.id_cliente,
    c.nome as nome_cliente,
    c.cidade as cidade_cliente,
    c.estado as estado_cliente,

    p.id_vendedor,
    v.nome as nome_vendedor,
    v.regiao,
    v.estado as estado_vendedor,

    i.quantidade,
    i.preco_unitario,
    i.desconto,
    i.valor_bruto,
    i.valor_desconto,
    i.valor_liquido,

    p.pedido_faturado,
    p.pedido_cancelado,

    i.created_at,
    i.updated_at

from {{ ref('fct_itens_pedido') }} i

inner join {{ ref('fct_pedidos') }} p
    on i.id_pedido = p.id_pedido

left join {{ ref('dim_produtos') }} pr
    on i.id_produto = pr.id_produto

left join {{ ref('dim_clientes') }} c
    on p.id_cliente = c.id_cliente

left join {{ ref('dim_vendedores') }} v
    on p.id_vendedor = v.id_vendedor