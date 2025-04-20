from utils import *
def analyse_data(spark):
    
    q1 = spark.sql("""
        SELECT 
            year(o.OrderDate) AS Year,
            p.Color,
            ROUND(SUM(d.OrderQty * (d.UnitPrice - d.UnitPriceDiscount)), 2) AS Revenue
        FROM store_orders d
        JOIN store_products p ON d.ProductID = p.ProductID
        JOIN store_orders o ON d.SalesOrderID = o.SalesOrderID
        GROUP BY year(o.OrderDate), p.Color
    """)
    q1.createOrReplaceTempView("color_revenue")

    
    highest_revenue_color = spark.sql("""
        SELECT Year, Color, Revenue
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Revenue DESC) AS rnk
            FROM color_revenue
        ) WHERE rnk = 1
    """)

    
    store_orders_with_leadtime = spark.sql("""
        SELECT *
        FROM store_orders
    """)

    store_orders_with_leadtime.createOrReplaceTempView("store_orders_with_leadtime")

   
    leadtime_avg = spark.sql("""
        SELECT 
            p.ProductCategoryName,
            ROUND(AVG(o.LeadTimeInBusinessDays), 2) AS AvgLeadTimeInBusinessDays
        FROM store_orders_with_leadtime o
        JOIN store_products p ON o.ProductID = p.ProductID
        GROUP BY p.ProductCategoryName
    """)

    return highest_revenue_color, leadtime_avg
