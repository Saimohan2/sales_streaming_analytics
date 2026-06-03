from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
import sys

catalog = sys.argv[1]
checkpoints_dir = sys.argv[2]

spark.conf.set("spark.sql.shuffle.partitions", 10)

spark = DatabricksSession.builder.getOrCreate()

df = spark.readStream.table(f"{catalog}.slv.expenses")

hourly_exp = (df.withWatermark("event_time", "2 hours")
                .dropDuplicates(["expense_id"])
                .groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
                .agg(F.count("*").alias("total_expenses"),
                     F.sum(F.col("expense_amount")).alias("total_spend"),
                     F.avg(F.col("expense_amount")).alias("avg_expense_per_event"))
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .withColumn("date", F.to_date("window_start"))
                .select("date", "window_start", "window_end", "region_id", "total_expenses",
                        "total_spend", "avg_expense_per_event"))

reg_df = spark.read.table(f"{catalog}.slv.regions")

joined_df = (hourly_exp.alias("h")
             .join(F.broadcast(reg_df).alias("r"), on = ["region_id"], how = "inner")
             .select("h.date", "h.window_start", "h.window_end", "r.city", "h.total_expenses",
                    "h.total_spend", "h.avg_expense_per_event"))

query = (joined_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"abfss://checkpoints@jayveeradlsdevtest.dfs.core.windows.net/{checkpoints_dir}/gld_checkpoints/reg_hrly_exp_win_agg_checkpoint")
            .outputMode("append")
            .trigger(availableNow = True)
            .table(f"{catalog}.gld.hourly_regional_expenses_summary_agg"))

query.awaitTermination()