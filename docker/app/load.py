from delta.tables import *
import os

def load_to_delta(df, table_name, storage_option='local'):
    spark = SparkSession.builder.getOrCreate()
    
    if storage_option == 'local':
        path = f"/opt/spark-apps/output/{table_name}"
        os.makedirs("/opt/spark-apps/output", exist_ok=True)
        df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(path)
        print(f"Data written to local: {path}")
    
    elif storage_option == 'minio':
        path = f"s3a://datalake/{table_name}"
        df.write.format("delta").mode("overwrite").save(path)
        print(f"Data written to MinIO: {path}")
    
    elif storage_option == 'both':
        local_path = f"/opt/spark-apps/output/{table_name}"
        minio_path = f"s3a://datalake/{table_name}"
        os.makedirs("/opt/spark-apps/output", exist_ok=True)
        
        df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(local_path)
        df.write.format("delta").mode("overwrite").save(minio_path)
        print(f"Data written to both local ({local_path}) and MinIO ({minio_path})")
    
    df.createOrReplaceTempView(table_name)