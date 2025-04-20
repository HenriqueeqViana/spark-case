from utils import *
def transform_products(df):
    return df.fillna({"Color": "N/A"}) \
        .withColumn("ProductID", F.col("ProductID").cast(IntegerType())) \
        .withColumn("ProductDesc", F.col("ProductDesc").cast(StringType())) \
        .withColumn("Color", F.col("Color").cast(StringType())) \
        .withColumn("ProductSubCategoryName", F.col("ProductSubCategoryName").cast(StringType())) \
        .withColumn("ProductCategoryName", 
            F.when(F.col("ProductCategoryName").isNotNull(), F.col("ProductCategoryName"))
             .when(F.col("ProductSubCategoryName").isin("Gloves", "Shorts", "Socks", "Tights", "Vests"), F.lit("Clothing"))
             .when(F.col("ProductSubCategoryName").isin("Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"), F.lit("Accessories"))
             .when(
                 (F.col("ProductSubCategoryName").contains("Frames")) |
                 (F.col("ProductSubCategoryName").isin("Wheels", "Saddles")),
                 F.lit("Components")
             ).otherwise(None)
        )
    
def transform_orders(detail_df, header_df,business_days_udf):
    detail_df = (
        detail_df.withColumn("SalesOrderID", F.col("SalesOrderID").cast(IntegerType()))
                 .withColumn("ProductID", F.col("ProductID").cast(IntegerType()))
                 .withColumn("OrderQty", F.col("OrderQty").cast(IntegerType()))
                 .withColumn("UnitPrice", F.col("UnitPrice").cast(DoubleType()))
                 .withColumn("UnitPriceDiscount", F.col("UnitPriceDiscount").cast(DoubleType()))
    )

    header_df = (
        header_df.withColumn("SalesOrderID", F.col("SalesOrderID").cast(IntegerType()))
                 .withColumn("OrderDate", F.col("OrderDate").cast(DateType()))
                 .withColumn("ShipDate", F.col("ShipDate").cast(DateType()))
                 .withColumn("Freight", F.col("Freight").cast(DoubleType()))
    )

    joined_df = (
    detail_df.alias("d")
    .join(header_df.alias("h"), F.col("d.SalesOrderID") == F.col("h.SalesOrderID"))
    .withColumn("LeadTimeInBusinessDays", business_days_udf(F.col("h.OrderDate"), F.col("h.ShipDate")))
    .withColumn("TotalLineExtendedPrice", F.col("OrderQty") * (F.col("UnitPrice") - F.col("UnitPriceDiscount")))
    )


    return joined_df.select(
        "d.*",
        *[col for col in header_df.columns if col != "SalesOrderID"],
        F.col("h.Freight").alias("TotalOrderFreight"),
        F.col("LeadTimeInBusinessDays"),
        F.col('TotalLineExtendedPrice')
    )
