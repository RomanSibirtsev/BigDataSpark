CREATE TABLE IF NOT EXISTS analytics.mart_products (
    product_name String,
    product_category String,
    sales_count UInt64,
    units_sold UInt64,
    revenue Decimal(18, 2),
    average_rating Nullable(Float64),
    average_reported_review_count Nullable(Decimal(18, 2)),
    sales_rank UInt32,
    category_revenue Decimal(18, 2)
) ENGINE = MergeTree ORDER BY (product_category, sales_rank, product_name);

CREATE TABLE IF NOT EXISTS analytics.mart_customers (
    customer_email String,
    country String,
    order_count UInt64,
    total_spent Decimal(18, 2),
    average_ticket Decimal(18, 2),
    spend_rank UInt32,
    country_customer_count UInt64
) ENGINE = MergeTree ORDER BY (spend_rank, country, customer_email);

CREATE TABLE IF NOT EXISTS analytics.mart_time (
    period_type LowCardinality(String),
    period_start Date,
    order_count UInt64,
    units_sold UInt64,
    revenue Decimal(18, 2),
    average_ticket Decimal(18, 2),
    previous_revenue Nullable(Decimal(18, 2)),
    revenue_change Nullable(Decimal(18, 2)),
    revenue_change_pct Nullable(Decimal(18, 6))
) ENGINE = MergeTree ORDER BY (period_type, period_start);

CREATE TABLE IF NOT EXISTS analytics.mart_stores (
    store_name String,
    city String,
    country String,
    order_count UInt64,
    revenue Decimal(18, 2),
    average_ticket Decimal(18, 2),
    revenue_rank UInt32,
    city_revenue Decimal(18, 2),
    country_revenue Decimal(18, 2)
) ENGINE = MergeTree ORDER BY (revenue_rank, country, city, store_name);

CREATE TABLE IF NOT EXISTS analytics.mart_suppliers (
    supplier_name String,
    supplier_country String,
    order_count UInt64,
    revenue Decimal(18, 2),
    average_product_price Decimal(18, 2),
    revenue_rank UInt32,
    country_revenue Decimal(18, 2)
) ENGINE = MergeTree ORDER BY (revenue_rank, supplier_country, supplier_name);

CREATE TABLE IF NOT EXISTS analytics.mart_quality (
    product_name String,
    product_category String,
    average_rating Nullable(Float64),
    average_reported_review_count Nullable(Decimal(18, 2)),
    units_sold UInt64,
    best_rating_rank UInt32,
    worst_rating_rank UInt32,
    review_count_rank UInt32,
    rating_sales_correlation Nullable(Float64)
) ENGINE = MergeTree ORDER BY (product_category, product_name);
