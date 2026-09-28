select
    f.id_pedido,

    -- Data
    f.data_pedido,
    f.ano_pedido,
    f.mes_pedido,
    f.dia_pedido,

    -- Cliente
    f.id_cliente,
    c.nome as nome_cliente,
    c.cidade as cidade_cliente,
    c.estado as estado_cliente,

    -- Vendedor
    f.id_vendedor,
    v.nome as nome_vendedor,
    v.regiao,
    v.estado as estado_vendedor,
    v.ativo as vendedor_ativo,

    -- Pedido
    f.status,
    f.valor_total,
    f.pedido_faturado,
    f.pedido_cancelado,

    -- Auditoria
    f.created_at,
    f.updated_at,

    case
        when pedido_faturado = 1 then valor_total
        else 0
    end as valor_faturado

from {{ ref('fct_pedidos') }} f

left join {{ ref('dim_clientes') }} c
    on f.id_cliente = c.id_cliente

left join {{ ref('dim_vendedores') }} v
    on f.id_vendedor = v.id_vendedor