from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
import sys

spark = DatabricksSession.builder.getOrCreate()

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", 10)

catalog = sys.argv[1]
checkpoints_dir = sys.argv[2]

df = (spark.readStream.table(f"{catalog}.slv.sales"))

hourly_df = (df.withWatermark("event_time", "2 hours")
                .dropDuplicates(["sales_id"])
                .groupBy(F.window("event_time", "1 hour"), F.col("region_id"))
                .agg(F.count("*").alias("total_orders"), 
                     F.approx_count_distinct(F.col("product_id")).alias("uniq_prods_sold"),
                     F.sum(F.col("quantity")).alias("total_units_sold"),
                     F.sum(F.col("sales_amount")).alias("total_revenue"),
                     F.avg(F.col("sales_amount")).alias("aov"),
                     F.avg(F.col("quantity")).alias("avg_uts_per_order"))
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .withColumn("date", F.to_date("window_start")))

hourly_df = hourly_df.select("date", "window_start", "window_end", "region_id", "total_orders",
                             "uniq_prods_sold", "total_units_sold", "total_revenue",
                             "aov", "avg_uts_per_order")

reg_df = (spark.read.table(f"{catalog}.slv.regions")
                .select("region_id", "city"))

joined_df = (hourly_df.alias("h")
                .join(F.broadcast(reg_df).alias("r"), on = ["region_id"], how = "inner")
                .select("h.date", "h.window_start", "h.window_end", "r.city", "h.total_orders",
                        "h.uniq_prods_sold", "h.total_units_sold", "h.total_revenue",
                        "h.aov", "h.avg_uts_per_order"))

query = (joined_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"abfss://checkpoints@jayveeradlsdevtest.dfs.core.windows.net/{checkpoints_dir}/gld_checkpoints/reg_hrly_win_agg_checkpoint")
            .outputMode("append")
            .trigger(availableNow = True)
            .table(f"{catalog}.gld.hourly_regional_sales_summary_agg"))

query.awaitTermination()