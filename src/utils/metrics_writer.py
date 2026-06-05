from pyspark.sql import functions as F
from datetime import datetime

def sales_slv_metrics(spark, job_id, task_name, run_id, job_start_ts, batch_id, 
                      source_df, good_sales_df, catalog):
    
    # persisting both the dataframes, so the upcoming multiple operations won't be needing a full scan again

    source_df = source_df.persist()

    good_sales_df = good_sales_df.persist()

    # track input rows count 

    incoming_records = source_df.count()

    # track output rows count

    output_rows = good_sales_df.count()

    # track null records coming in critical columns

    null_records = (source_df.agg(F.sum(F.when(F.col("sales_id").isNull(), 1)\
                                          .otherwise(0)).alias("null_sales_id"),
                                    F.sum(F.when(F.col("employee_id").isNull(), 1)\
                                          .otherwise(0)).alias("null_employee_id"),
                                    F.sum(F.when(F.col("region_id").isNull(), 1)\
                                          .otherwise(0)).alias("null_region_id"))
                            .collect()[0])
    
    # track incoming negative sales values
    
    negative_records = (source_df.agg(F.sum(F.when(F.col("sales_amount")<0, 1)\
                                              .otherwise(0)).alias("negative_sales_amount"))
                                .collect()[0])

    # store metrics to a dict

    metrics_dict = {
        "job_id": job_id,
        "task_name": task_name,
        "run_id": run_id,
        "batch_id": batch_id,
        "job_start_ts": job_start_ts,
        "run_date": datetime.now().date(),
        "pipeline_stage": "brz_to_slv_sales",
        "source_table": f"{catalog}.brz.sales_raw",
        "target_table": f"{catalog}.slv.sales",
        "incoming_records": incoming_records,
        "null_sales_id": null_records["null_sales_id"],
        "null_employee_id": null_records["null_employee_id"],
        "null_region_id": null_records["null_region_id"],
        "negative_sales_amount": negative_records["negative_sales_amount"],
        "output_records": output_rows,
        "status": "success"
    }
    
    # create dataframe to write to table

    metrics_df = spark.createDataFrame([metrics_dict])

    metrics_df = metrics_df.select(F.col("job_id").cast("long"), "task_name", 
                                   F.col("run_id").cast("long"), "batch_id",
                                   F.from_unixtime(F.col("job_start_ts") / 1000)\
                                    .cast("timestamp").alias("job_start_ts"),
                                   "run_date", "pipeline_stage", "source_table",
                                   "target_table", "incoming_records", "null_sales_id",
                                   "null_employee_id", "null_region_id", "negative_sales_amount",
                                   "output_records", "status")

    # write to metrics dataframe in append mode

    metrics_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.slv.sales_metrics")

    source_df.unpersist()
    good_sales_df.unpersist()