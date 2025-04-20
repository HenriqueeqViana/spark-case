def extract(spark, file_name):
    df = spark.read.option("header", True).option("inferSchema", True).csv(f"/opt/spark-apps/data/{file_name}.csv")
    df.createOrReplaceTempView(f"raw_{file_name.replace('-', '_')}")
    return df