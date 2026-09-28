select
    cast(id_cliente as int64) as id_cliente,
    trim(nome) as nome,
    lower(trim(email)) as email,
    trim(cidade) as cidade,
    upper(trim(estado)) as estado,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from {{ source('bronze', 'clientes') }}