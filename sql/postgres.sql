CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS star;

CREATE TABLE IF NOT EXISTS raw.mock_data (
    id TEXT, customer_first_name TEXT, customer_last_name TEXT, customer_age TEXT,
    customer_email TEXT, customer_country TEXT, customer_postal_code TEXT,
    customer_pet_type TEXT, customer_pet_name TEXT, customer_pet_breed TEXT,
    seller_first_name TEXT, seller_last_name TEXT, seller_email TEXT,
    seller_country TEXT, seller_postal_code TEXT, product_name TEXT,
    product_category TEXT, product_price TEXT, product_quantity TEXT,
    sale_date TEXT, sale_customer_id TEXT, sale_seller_id TEXT,
    sale_product_id TEXT, sale_quantity TEXT, sale_total_price TEXT,
    store_name TEXT, store_location TEXT, store_city TEXT, store_state TEXT,
    store_country TEXT, store_phone TEXT, store_email TEXT, pet_category TEXT,
    product_weight TEXT, product_color TEXT, product_size TEXT,
    product_brand TEXT, product_material TEXT, product_description TEXT,
    product_rating TEXT, product_reviews TEXT, product_release_date TEXT,
    product_expiry_date TEXT, supplier_name TEXT, supplier_contact TEXT,
    supplier_email TEXT, supplier_phone TEXT, supplier_address TEXT,
    supplier_city TEXT, supplier_country TEXT,
    source_file TEXT NOT NULL, source_row TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS star.dim_customer (
    customer_key VARCHAR(64) PRIMARY KEY,
    first_name TEXT, last_name TEXT, age INTEGER, email TEXT, country TEXT,
    postal_code TEXT, pet_type TEXT, pet_name TEXT, pet_breed TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_seller (
    seller_key VARCHAR(64) PRIMARY KEY,
    first_name TEXT, last_name TEXT, email TEXT, country TEXT, postal_code TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_product (
    product_key VARCHAR(64) PRIMARY KEY,
    source_product_id INTEGER, name TEXT, category TEXT, price NUMERIC(18,2),
    stock_quantity INTEGER, pet_category TEXT, weight NUMERIC(18,3),
    color TEXT, size TEXT, brand TEXT, material TEXT, description TEXT,
    rating NUMERIC(6,2), reviews INTEGER, release_date DATE, expiry_date DATE
);

CREATE TABLE IF NOT EXISTS star.dim_store (
    store_key VARCHAR(64) PRIMARY KEY,
    name TEXT, location TEXT, city TEXT, state TEXT, country TEXT,
    phone TEXT, email TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_supplier (
    supplier_key VARCHAR(64) PRIMARY KEY,
    name TEXT, contact TEXT, email TEXT, phone TEXT, address TEXT,
    city TEXT, country TEXT
);

CREATE TABLE IF NOT EXISTS star.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL,
    quarter INTEGER NOT NULL, iso_week INTEGER NOT NULL,
    iso_day_of_week INTEGER NOT NULL, is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS star.fact_sales (
    sale_key VARCHAR(64) PRIMARY KEY,
    source_file TEXT NOT NULL, source_row TEXT NOT NULL,
    customer_key VARCHAR(64) REFERENCES star.dim_customer(customer_key),
    seller_key VARCHAR(64) REFERENCES star.dim_seller(seller_key),
    product_key VARCHAR(64) REFERENCES star.dim_product(product_key),
    store_key VARCHAR(64) REFERENCES star.dim_store(store_key),
    supplier_key VARCHAR(64) REFERENCES star.dim_supplier(supplier_key),
    date_key INTEGER REFERENCES star.dim_date(date_key),
    sale_date DATE, source_customer_id INTEGER, source_seller_id INTEGER,
    source_product_id INTEGER, quantity INTEGER,
    total_price NUMERIC(18,2)
);

CREATE INDEX IF NOT EXISTS fact_sales_date_idx ON star.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS fact_sales_customer_idx ON star.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS fact_sales_product_idx ON star.fact_sales(product_key);
