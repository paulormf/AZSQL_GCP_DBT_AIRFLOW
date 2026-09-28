INSERT INTO dbo.clientes
(
    id_cliente,
    nome,
    email,
    cidade,
    estado,
    created_at,
    updated_at
)
SELECT TOP (9999)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 1,
    CONCAT('Cliente ', ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 1),
    CONCAT(
        'cliente',
        ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + 1,
        '@email.com'
    ),
    CASE
        WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 5 = 0 THEN 'São Paulo'
        WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 5 = 1 THEN 'Santo André'
        WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 5 = 2 THEN 'São Bernardo do Campo'
        WHEN ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) % 5 = 3 THEN 'Osasco'
        ELSE 'Guarulhos'
    END,
    'SP',
    GETDATE(),
    GETDATE()
FROM sys.all_objects a
CROSS JOIN sys.all_objects b;