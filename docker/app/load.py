from delta.tables import *
import os

def load_to_delta(df, table_name):
    
    path_local = f"/opt/spark-apps/output/{table_name}"  
    os.makedirs("/opt/spark-apps/output", exist_ok=True)

    
    df.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(path_local)

    
    df.createOrReplaceTempView(table_name)

    
    # path_s3 = f"s3a://datalake/{table_name}"
    # df.write.format("delta").mode("overwrite").save(path_s3)

    print(f"Data written to {path_local} (local).")