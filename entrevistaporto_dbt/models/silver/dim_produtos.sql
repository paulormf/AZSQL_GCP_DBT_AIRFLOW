select
    id_produto,
    nome,
    categoria,
    marca,
    preco,
    ativo,
    created_at,
    updated_at

from {{ ref('stg_produtos') }}