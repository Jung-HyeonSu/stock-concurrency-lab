CREATE TABLE IF NOT EXISTS product (
    id      BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name    VARCHAR(100) NOT NULL,
    stock   INT          NOT NULL,
    version BIGINT       NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id         BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    product_id BIGINT       NOT NULL,
    user_id    BIGINT       NOT NULL,
    status     VARCHAR(20)  NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    CONSTRAINT uk_orders_product_user UNIQUE (product_id, user_id)
);
