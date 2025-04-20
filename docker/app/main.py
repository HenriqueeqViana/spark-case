from pyspark.sql import SparkSession
from extract import extract
from transform import transform_products, transform_orders
from load import load_to_delta
from analyse import analyse_data
from utils import  *
from pyspark.sql.types import DateType,IntegerType
import datetime


def calculate_business_days(start, end):
        if start is None or end is None:
            return None
        delta = (end - start).days + 1
        return sum(1 for i in range(delta) if (start + datetime.timedelta(days=i)).weekday() < 5)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ETL Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.0.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars", "/opt/bitnami/spark/jars/delta-core_2.12-1.0.0.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.log.level", "INFO") \
        .getOrCreate()
    business_days_udf =spark.udf.register("business_days_udf", calculate_business_days, IntegerType())
    sales_header = extract(spark, "sales-order-header-1-1-")
    sales_detail = extract(spark, "sales-order-detail-1-1-")
    products = extract(spark, "products-1-1-")
    load_to_delta(sales_header, "raw_products")
    load_to_delta(sales_detail, "raw_orders")
    load_to_delta(products, "raw_orders")

    products_df = transform_products(products)
    orders_df = transform_orders(sales_detail, sales_header, business_days_udf)


    load_to_delta(products_df, "store_products")
    load_to_delta(orders_df, "store_orders")

    highest_revenue_df, avg_leadtime_df = analyse_data(spark)

    load_to_delta(highest_revenue_df, "analysis_highest_revenue_color")
    load_to_delta(avg_leadtime_df, "analysis_avg_leadtime_by_category")

    spark.stop()