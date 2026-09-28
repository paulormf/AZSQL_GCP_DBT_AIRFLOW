CREATE TABLE clientes (
    id_cliente INT NOT NULL PRIMARY KEY,
    nome VARCHAR(150) NOT NULL,
    email VARCHAR(150),
    cidade VARCHAR(100),
    estado CHAR(2),
    created_at DATETIME2 NOT NULL,
    updated_at DATETIME2 NOT NULL
);