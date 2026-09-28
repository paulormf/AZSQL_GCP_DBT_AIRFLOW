select
    id_cliente,
    nome,
    to_hex(sha256(cast(email as bytes))) as email_hash,
    cidade,
    estado,
    created_at,
    updated_at

from {{ ref('stg_clientes') }}