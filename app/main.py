from __future__ import annotations

import base64
import json
import math
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen

import psycopg2
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


SOURCE_COLUMNS = [
    "id", "customer_first_name", "customer_last_name", "customer_age",
    "customer_email", "customer_country", "customer_postal_code",
    "customer_pet_type", "customer_pet_name", "customer_pet_breed",
    "seller_first_name", "seller_last_name", "seller_email",
    "seller_country", "seller_postal_code", "product_name",
    "product_category", "product_price", "product_quantity", "sale_date",
    "sale_customer_id", "sale_seller_id", "sale_product_id", "sale_quantity",
    "sale_total_price", "store_name", "store_location", "store_city",
    "store_state", "store_country", "store_phone", "store_email",
    "pet_category", "product_weight", "product_color", "product_size",
    "product_brand", "product_material", "product_description",
    "product_rating", "product_reviews", "product_release_date",
    "product_expiry_date", "supplier_name", "supplier_contact",
    "supplier_email", "supplier_phone", "supplier_address", "supplier_city",
    "supplier_country",
]

def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def jdbc_settings() -> tuple[str, dict[str, str]]:
    url = (
        f"jdbc:postgresql://{env('PG_HOST', 'localhost')}:{env('PG_PORT', '5432')}"
        f"/{env('PG_DATABASE', 'bigdata')}"
    )
    props = {
        "user": env("PG_USER", "lab"),
        "password": env("PG_PASSWORD", "lab"),
        "driver": "org.postgresql.Driver",
        "batchsize": "1000",
    }
    return url, props


def replace_postgres_contents() -> None:
    with psycopg2.connect(
        host=env("PG_HOST", "localhost"),
        port=env("PG_PORT", "5432"),
        dbname=env("PG_DATABASE", "bigdata"),
        user=env("PG_USER", "lab"),
        password=env("PG_PASSWORD", "lab"),
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE raw.mock_data, star.fact_sales, star.dim_customer, "
                "star.dim_seller, star.dim_product, star.dim_store, "
                "star.dim_supplier, star.dim_date RESTART IDENTITY CASCADE"
            )


def write_postgres(df: DataFrame, table: str) -> None:
    jdbc_url, properties = jdbc_settings()
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .options(**properties)
        .mode("append")
        .save()
    )


def read_postgres(spark: SparkSession, table: str) -> DataFrame:
    jdbc_url, properties = jdbc_settings()
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .options(**properties)
        .load()
    )


def hash_key(df: DataFrame, columns: list[str], output: str) -> DataFrame:
    parts = [F.coalesce(F.col(name).cast("string"), F.lit("<null>")) for name in columns]
    return df.withColumn(output, F.sha2(F.concat_ws("\u001f", *parts), 256))


def alias_columns(df: DataFrame, key_name: str, key_source: str, names: dict[str, str]) -> DataFrame:
    selected = [F.col(key_source).alias(key_name)]
    selected.extend(F.col(source).alias(target) for source, target in names.items())
    return df.select(*selected).dropDuplicates([key_name])


def make_raw_frame(spark: SparkSession) -> DataFrame:
    files = env("SOURCE_PATH", "/data/*.csv")
    source = (
        spark.read.format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("emptyValue", "")
        .load(files)
    )
    absent = sorted(set(SOURCE_COLUMNS) - set(source.columns))
    if absent:
        raise ValueError(f"CSV is missing expected fields: {', '.join(absent)}")

    source = source.withColumn(
        "_file_name", F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
    )
    raw = source.select(
        *[F.col(name).cast("string").alias(name) for name in SOURCE_COLUMNS],
        F.col("_file_name").alias("source_file"),
        F.col("id").cast("string").alias("source_row"),
    )
    return raw


def cast_source(raw: DataFrame) -> DataFrame:
    integer_fields = [
        "customer_age", "product_quantity", "sale_customer_id", "sale_seller_id",
        "sale_product_id", "sale_quantity", "product_reviews",
    ]
    decimal_fields = ["product_price", "product_weight", "product_rating", "sale_total_price"]
    date_fields = ["sale_date", "product_release_date", "product_expiry_date"]

    typed = raw
    non_text_fields = set(integer_fields + decimal_fields + date_fields)
    for name in SOURCE_COLUMNS:
        if name not in non_text_fields:
            typed = typed.withColumn(name, F.trim(F.col(name)))
    for name in ("customer_postal_code", "seller_postal_code", "store_state"):
        typed = typed.withColumn(
            name,
            F.when(F.col(name) == "", F.lit(None).cast("string")).otherwise(F.col(name)),
        )
    for name in integer_fields:
        typed = typed.withColumn(name, F.col(name).cast("integer"))
    for name in decimal_fields:
        typed = typed.withColumn(name, F.col(name).cast("decimal(18,3)" if name == "product_weight" else "decimal(18,2)"))
    for name in date_fields:
        typed = typed.withColumn(name, F.to_date(F.col(name), "M/d/yyyy"))
    return typed


def build_star_frames(raw: DataFrame) -> dict[str, DataFrame]:
    typed = cast_source(raw)
    groups = {
        "customer": [
            "customer_first_name", "customer_last_name", "customer_age", "customer_email",
            "customer_country", "customer_postal_code", "customer_pet_type",
            "customer_pet_name", "customer_pet_breed",
        ],
        "seller": [
            "seller_first_name", "seller_last_name", "seller_email",
            "seller_country", "seller_postal_code",
        ],
        "product": [
            "sale_product_id", "product_name", "product_category", "product_price",
            "product_quantity", "pet_category", "product_weight", "product_color",
            "product_size", "product_brand", "product_material", "product_description",
            "product_rating", "product_reviews", "product_release_date", "product_expiry_date",
        ],
        "store": [
            "store_name", "store_location", "store_city", "store_state", "store_country",
            "store_phone", "store_email",
        ],
        "supplier": [
            "supplier_name", "supplier_contact", "supplier_email", "supplier_phone",
            "supplier_address", "supplier_city", "supplier_country",
        ],
    }
    for label, attributes in groups.items():
        typed = hash_key(typed, attributes, f"_{label}_key")

    dimensions = {
        "star.dim_customer": alias_columns(
            typed, "customer_key", "_customer_key", {
                "customer_first_name": "first_name", "customer_last_name": "last_name",
                "customer_age": "age", "customer_email": "email", "customer_country": "country",
                "customer_postal_code": "postal_code", "customer_pet_type": "pet_type",
                "customer_pet_name": "pet_name", "customer_pet_breed": "pet_breed",
            },
        ),
        "star.dim_seller": alias_columns(
            typed, "seller_key", "_seller_key", {
                "seller_first_name": "first_name", "seller_last_name": "last_name",
                "seller_email": "email", "seller_country": "country",
                "seller_postal_code": "postal_code",
            },
        ),
        "star.dim_product": alias_columns(
            typed, "product_key", "_product_key", {
                "sale_product_id": "source_product_id", "product_name": "name",
                "product_category": "category", "product_price": "price",
                "product_quantity": "stock_quantity", "pet_category": "pet_category",
                "product_weight": "weight", "product_color": "color", "product_size": "size",
                "product_brand": "brand", "product_material": "material",
                "product_description": "description", "product_rating": "rating",
                "product_reviews": "reviews", "product_release_date": "release_date",
                "product_expiry_date": "expiry_date",
            },
        ),
        "star.dim_store": alias_columns(
            typed, "store_key", "_store_key", {
                "store_name": "name", "store_location": "location", "store_city": "city",
                "store_state": "state", "store_country": "country", "store_phone": "phone",
                "store_email": "email",
            },
        ),
        "star.dim_supplier": alias_columns(
            typed, "supplier_key", "_supplier_key", {
                "supplier_name": "name", "supplier_contact": "contact",
                "supplier_email": "email", "supplier_phone": "phone",
                "supplier_address": "address", "supplier_city": "city",
                "supplier_country": "country",
            },
        ),
    }

    dates = (
        typed.select("sale_date").where(F.col("sale_date").isNotNull()).dropDuplicates()
        .withColumn("date_key", F.date_format("sale_date", "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year("sale_date"))
        .withColumn("month", F.month("sale_date"))
        .withColumn("day", F.dayofmonth("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
        .withColumn("iso_week", F.weekofyear("sale_date"))
        .withColumn(
            "iso_day_of_week",
            ((F.dayofweek("sale_date") + 5) % 7 + 1).cast("integer"),
        )
        .withColumn("is_weekend", F.dayofweek("sale_date").isin(1, 7))
        .select(
            F.col("date_key"), F.col("sale_date").alias("full_date"),
            "year", "month", "day", "quarter", "iso_week",
            "iso_day_of_week", "is_weekend",
        )
    )
    dimensions["star.dim_date"] = dates

    fact_source = hash_key(typed, ["source_file", "source_row"], "sale_key")
    fact = fact_source.select(
        "sale_key", "source_file", "source_row",
        F.col("_customer_key").alias("customer_key"),
        F.col("_seller_key").alias("seller_key"),
        F.col("_product_key").alias("product_key"),
        F.col("_store_key").alias("store_key"),
        F.col("_supplier_key").alias("supplier_key"),
        F.date_format("sale_date", "yyyyMMdd").cast("integer").alias("date_key"),
        "sale_date", F.col("sale_customer_id").alias("source_customer_id"),
        F.col("sale_seller_id").alias("source_seller_id"),
        F.col("sale_product_id").alias("source_product_id"),
        F.col("sale_quantity").alias("quantity"),
        F.col("sale_total_price").alias("total_price"),
    )
    return {**dimensions, "star.fact_sales": fact}


def persist_star(raw: DataFrame, frames: dict[str, DataFrame]) -> None:
    replace_postgres_contents()
    write_postgres(raw, "raw.mock_data")
    load_order = [
        "star.dim_customer", "star.dim_seller", "star.dim_product",
        "star.dim_store", "star.dim_supplier", "star.dim_date", "star.fact_sales",
    ]
    for table in load_order:
        write_postgres(frames[table], table)


def joined_star(spark: SparkSession) -> DataFrame:
    fact = read_postgres(spark, "star.fact_sales").alias("f")
    product = read_postgres(spark, "star.dim_product").alias("p")
    customer = read_postgres(spark, "star.dim_customer").alias("c")
    store = read_postgres(spark, "star.dim_store").alias("st")
    supplier = read_postgres(spark, "star.dim_supplier").alias("su")
    return (
        fact.join(product, F.col("f.product_key") == F.col("p.product_key"))
        .join(customer, F.col("f.customer_key") == F.col("c.customer_key"))
        .join(store, F.col("f.store_key") == F.col("st.store_key"))
        .join(supplier, F.col("f.supplier_key") == F.col("su.supplier_key"))
    )


def product_mart(sales: DataFrame) -> DataFrame:
    grouped = sales.groupBy("p.name", "p.category").agg(
        F.count(F.lit(1)).cast("long").alias("sales_count"),
        F.sum("f.quantity").cast("long").alias("units_sold"),
        F.sum("f.total_price").cast("decimal(18,2)").alias("revenue"),
        F.avg("p.rating").cast("double").alias("average_rating"),
        F.avg("p.reviews").cast("decimal(18,2)").alias("average_reported_review_count"),
    ).select(
        F.col("name").alias("product_name"), F.col("category").alias("product_category"),
        "sales_count", "units_sold", "revenue", "average_rating",
        "average_reported_review_count",
    )
    rank = Window.orderBy(F.col("units_sold").desc(), "product_category", "product_name")
    category = Window.partitionBy("product_category")
    return (
        grouped.withColumn("sales_rank", F.row_number().over(rank))
        .withColumn("category_revenue", F.sum("revenue").over(category).cast("decimal(18,2)"))
    )


def customer_mart(sales: DataFrame) -> DataFrame:
    grouped = sales.groupBy(
        F.coalesce(F.col("c.email"), F.lit("unknown")).alias("customer_email"),
        F.coalesce(F.col("c.country"), F.lit("unknown")).alias("country"),
    ).agg(
        F.count(F.lit(1)).cast("long").alias("order_count"),
        F.sum("f.total_price").cast("decimal(18,2)").alias("total_spent"),
    ).withColumn(
        "average_ticket", F.round(F.col("total_spent") / F.col("order_count"), 2)
        .cast("decimal(18,2)"),
    )
    rank = Window.orderBy(F.col("total_spent").desc(), "customer_email", "country")
    country = Window.partitionBy("country")
    return (
        grouped.withColumn("spend_rank", F.row_number().over(rank))
        .withColumn("country_customer_count", F.count(F.lit(1)).over(country).cast("long"))
    )


def time_mart(sales: DataFrame) -> DataFrame:
    periods: list[DataFrame] = []
    for label, pattern in (("month", "month"), ("year", "year")):
        period_start = F.date_trunc(pattern, F.col("f.sale_date")).cast(DateType())
        grouped = sales.groupBy(period_start.alias("period_start")).agg(
            F.count(F.lit(1)).cast("long").alias("order_count"),
            F.sum("f.quantity").cast("long").alias("units_sold"),
            F.sum("f.total_price").cast("decimal(18,2)").alias("revenue"),
        ).withColumn("period_type", F.lit(label)).withColumn(
            "average_ticket", F.round(F.col("revenue") / F.col("order_count"), 2)
            .cast("decimal(18,2)"),
        )
        periods.append(grouped)

    combined = periods[0].unionByName(periods[1])
    order = Window.partitionBy("period_type").orderBy("period_start")
    with_previous = combined.withColumn("previous_revenue", F.lag("revenue").over(order))
    with_change = with_previous.withColumn(
        "revenue_change", (F.col("revenue") - F.col("previous_revenue")).cast("decimal(18,2)")
    )
    return with_change.withColumn(
        "revenue_change_pct",
        F.when(
            F.col("previous_revenue").isNotNull() & (F.col("previous_revenue") != 0),
            F.round(F.col("revenue_change") / F.col("previous_revenue") * 100, 6)
            .cast("decimal(18,6)"),
        ),
    ).select(
        "period_type", "period_start", "order_count", "units_sold", "revenue",
        "average_ticket", "previous_revenue", "revenue_change", "revenue_change_pct",
    )


def store_mart(sales: DataFrame) -> DataFrame:
    grouped = sales.groupBy(
        F.coalesce(F.col("st.name"), F.lit("unknown")).alias("store_name"),
        F.coalesce(F.col("st.city"), F.lit("unknown")).alias("city"),
        F.coalesce(F.col("st.country"), F.lit("unknown")).alias("country"),
    ).agg(
        F.count(F.lit(1)).cast("long").alias("order_count"),
        F.sum("f.total_price").cast("decimal(18,2)").alias("revenue"),
    ).withColumn(
        "average_ticket", F.round(F.col("revenue") / F.col("order_count"), 2)
        .cast("decimal(18,2)"),
    )
    rank = Window.orderBy(F.col("revenue").desc(), "country", "city", "store_name")
    return (
        grouped.withColumn("revenue_rank", F.row_number().over(rank))
        .withColumn("city_revenue", F.sum("revenue").over(Window.partitionBy("city", "country")).cast("decimal(18,2)"))
        .withColumn("country_revenue", F.sum("revenue").over(Window.partitionBy("country")).cast("decimal(18,2)"))
    )


def supplier_mart(sales: DataFrame) -> DataFrame:
    grouped = sales.groupBy(
        F.coalesce(F.col("su.name"), F.lit("unknown")).alias("supplier_name"),
        F.coalesce(F.col("su.country"), F.lit("unknown")).alias("supplier_country"),
    ).agg(
        F.count(F.lit(1)).cast("long").alias("order_count"),
        F.sum("f.total_price").cast("decimal(18,2)").alias("revenue"),
        F.avg("p.price").cast("decimal(18,2)").alias("average_product_price"),
    )
    rank = Window.orderBy(F.col("revenue").desc(), "supplier_country", "supplier_name")
    return (
        grouped.withColumn("revenue_rank", F.row_number().over(rank))
        .withColumn(
            "country_revenue",
            F.sum("revenue").over(Window.partitionBy("supplier_country")).cast("decimal(18,2)"),
        )
    )


def quality_mart(sales: DataFrame) -> DataFrame:
    grouped = sales.groupBy("p.name", "p.category").agg(
        F.avg("p.rating").cast("double").alias("average_rating"),
        F.avg("p.reviews").cast("decimal(18,2)").alias("average_reported_review_count"),
        F.sum("f.quantity").cast("long").alias("units_sold"),
    ).select(
        F.col("name").alias("product_name"), F.col("category").alias("product_category"),
        "average_rating", "average_reported_review_count", "units_sold",
    )
    correlation = grouped.stat.corr("average_rating", "units_sold")
    high = Window.orderBy(F.col("average_rating").desc_nulls_last(), "product_name", "product_category")
    low = Window.orderBy(F.col("average_rating").asc_nulls_last(), "product_name", "product_category")
    reviews = Window.orderBy(
        F.col("average_reported_review_count").desc(), "product_name", "product_category"
    )
    return (
        grouped.withColumn("best_rating_rank", F.row_number().over(high))
        .withColumn("worst_rating_rank", F.row_number().over(low))
        .withColumn("review_count_rank", F.row_number().over(reviews))
        .withColumn("rating_sales_correlation", F.lit(correlation).cast("double"))
    )


def make_marts(spark: SparkSession) -> dict[str, DataFrame]:
    sales = joined_star(spark)
    return {
        "mart_products": product_mart(sales),
        "mart_customers": customer_mart(sales),
        "mart_time": time_mart(sales),
        "mart_stores": store_mart(sales),
        "mart_suppliers": supplier_mart(sales),
        "mart_quality": quality_mart(sales),
    }


def clickhouse_request(sql: str, body: str | None = None) -> None:
    base_url = env("CLICKHOUSE_URL", "http://localhost:8123/")
    user = env("CLICKHOUSE_USER", "lab")
    password = env("CLICKHOUSE_PASSWORD", "lab")
    credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
    separator = "&" if "?" in base_url else "?"
    query_url = f"{base_url}{separator}query={quote(sql, safe='')}"
    request = Request(
        query_url,
        data=(body or "").encode("utf-8"),
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "text/plain; charset=utf-8",
        },
        method="POST",
    )
    with urlopen(request, timeout=120) as response:
        response.read()


def ensure_clickhouse_schema() -> None:
    ddl = Path("/app/sql/clickhouse.sql").read_text(encoding="utf-8")
    for statement in ddl.split(";"):
        if statement.strip():
            clickhouse_request(statement.strip())


def json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def write_clickhouse(table: str, df: DataFrame, batch_size: int = 1000) -> int:
    database = env("CLICKHOUSE_DATABASE", "analytics")
    clickhouse_request(f"TRUNCATE TABLE {database}.{table}")
    batch: list[str] = []
    total = 0
    for row in df.toLocalIterator():
        record = {name: json_value(value) for name, value in row.asDict().items()}
        batch.append(json.dumps(record, ensure_ascii=False, allow_nan=False))
        if len(batch) >= batch_size:
            clickhouse_request(
                f"INSERT INTO {database}.{table} FORMAT JSONEachRow",
                "\n".join(batch) + "\n",
            )
            total += len(batch)
            batch.clear()
    if batch:
        clickhouse_request(
            f"INSERT INTO {database}.{table} FORMAT JSONEachRow",
            "\n".join(batch) + "\n",
        )
        total += len(batch)
    return total


def main() -> None:
    spark = (
        SparkSession.builder.appName("PetShopSalesETL")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = make_raw_frame(spark)
        raw_count = raw.count()
        star_frames = build_star_frames(raw)
        sale_count = star_frames["star.fact_sales"].count()
        persist_star(raw, star_frames)

        marts = make_marts(spark)
        ensure_clickhouse_schema()
        totals = {name: write_clickhouse(name, frame) for name, frame in marts.items()}
        print(
            "ETL_SUCCESS "
            f"raw={raw_count} star={sale_count} "
            f"marts={len(totals)} rows={sum(totals.values())}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
