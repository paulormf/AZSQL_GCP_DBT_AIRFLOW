select
    id_vendedor,
    nome,
    regiao,
    estado,
    ativo,
    created_at,
    updated_at
from {{ ref('stg_vendedores') }}