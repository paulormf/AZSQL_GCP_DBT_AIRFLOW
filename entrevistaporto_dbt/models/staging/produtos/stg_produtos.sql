select
    cast(id_produto as int64) as id_produto,
    trim(nome) as nome,
    trim(categoria) as categoria,
    trim(marca) as marca,
    cast(preco as numeric) as preco,
    cast(ativo as bool) as ativo,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from {{ source('bronze', 'produtos') }}