CREATE TABLE IF NOT EXISTS customers
(
    id         SERIAL PRIMARY KEY,
    first_name VARCHAR(50)  NOT NULL,
    last_name  VARCHAR(50)  NOT NULL,
    email      VARCHAR(100) NOT NULL
);
ALTER TABLE customers REPLICA IDENTITY FULL; -- needed for Debezium 'before' on UPDATE/DELETE

CREATE TABLE IF NOT EXISTS shipping_address
(
    id          SERIAL PRIMARY KEY,
    customer_id INT          NOT NULL REFERENCES customers (id) ON DELETE CASCADE,
    street      VARCHAR(200) NOT NULL,
    city        VARCHAR(100) NOT NULL,
    zip_code    VARCHAR(20)  NOT NULL
);
ALTER TABLE shipping_address REPLICA IDENTITY FULL;
