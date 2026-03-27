CREATE TABLE IF NOT EXISTS address (
    address_id SERIAL PRIMARY KEY,
    address VARCHAR(255),
    zipcode VARCHAR(10),
    state VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS event (
    event_id SERIAL PRIMARY KEY,
    session_id VARCHAR(255),
    user_id VARCHAR(255),
    page_url VARCHAR(255),
    created_at TIMESTAMP,
    event_type VARCHAR(100),
    order_id VARCHAR(255),
    product_id VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(255),
    product_id VARCHAR(255),
    quantity INTEGER
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(255) PRIMARY KEY,
    user_id VARCHAR(255),
    promo_id VARCHAR(255),
    address_id VARCHAR(255),
    created_at TIMESTAMP,
    order_cost DECIMAL(10, 2),
    shipping_cost DECIMAL(10, 2),
    order_total DECIMAL(10, 2),
    tracking_id VARCHAR(255),
    shipping_service VARCHAR(100),
    estimated_delivery_at TIMESTAMP,
    delivered_at TIMESTAMP,
    status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255),
    price DECIMAL(10, 2),
    inventory INTEGER
);

CREATE TABLE IF NOT EXISTS promos (
    promo_id VARCHAR(255) PRIMARY KEY,
    discount DECIMAL(5, 2),
    status VARCHAR(100)
);

create table if not exists users (
    user_id VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(20),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    address_id VARCHAR(255)
);