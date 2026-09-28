{{ config(
    materialized='incremental',
    unique_key='id_pedido',
    incremental_strategy='merge',
    partition_by={
        "field": "data_pedido",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=[
        "id_cliente",
        "id_vendedor"
    ]
) }}

select
    id_pedido,
    id_cliente,
    id_vendedor,

    data_pedido,
    extract(year from data_pedido) as ano_pedido,
    extract(month from data_pedido) as mes_pedido,
    extract(day from data_pedido) as dia_pedido,

    status,
    valor_total,

    case
        when upper(trim(status)) = 'FATURADO' then 1
        else 0
    end as pedido_faturado,

    case
        when upper(trim(status)) = 'CANCELADO' then 1
        else 0
    end as pedido_cancelado,

    created_at,
    updated_at

from {{ ref('stg_pedidos') }}

{% if is_incremental() %}

where updated_at >= timestamp_sub(
    coalesce(
        (select max(updated_at) from {{ this }}),
        timestamp('1900-01-01 00:00:00+00')
    ),
    interval 3 day
)

{% endif %}