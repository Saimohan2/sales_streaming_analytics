from pyspark.sql import functions as F

def good_sales_records(spark, df):

    df = (df.filter((F.col("sales_id").isNotNull()) & (F.col("employee_id").isNotNull()) &
                    (F.col("region_id").isNotNull()) & (F.col("sales_amount").isNotNull()) &
                    (F.col("sales_amount")>0))
                    .withColumn("processed_time",F.current_timestamp()))
    
    return df

def bad_sales_records(spark, df):

    df = (df.filter((F.col("sales_id").isNull()) | (F.col("employee_id").isNull()) |
                    (F.col("region_id").isNull()) | (F.col("sales_amount").isNull()) |
                    (F.col("sales_amount")<=0))
                    .withColumn("processed_time",F.current_timestamp()))
    
    return df