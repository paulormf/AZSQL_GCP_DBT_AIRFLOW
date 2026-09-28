select
    cast(id_vendedor as int64) as id_vendedor,
    trim(nome) as nome,
    trim(regiao) as regiao,
    upper(trim(estado)) as estado,
    cast(ativo as bool) as ativo,
    cast(created_at as timestamp) as created_at,
    cast(updated_at as timestamp) as updated_at

from {{ source('bronze', 'vendedores') }}