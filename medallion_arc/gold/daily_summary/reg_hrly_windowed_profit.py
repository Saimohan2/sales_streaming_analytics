from databricks.connect import DatabricksSession
from src.utils.agg_utils import calculate_profit, profit_margin
from pyspark.sql import functions as F
import sys

spark = DatabricksSession.builder.getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 6)

catalog = sys.argv[1]
checkpoints_dir = sys.argv[2]

sales_df = spark.readStream.table(f"{catalog}.slv.sales")

exp_df = spark.readStream.table(f"{catalog}.slv.expenses")

sales_norm = (sales_df.select("region_id", "event_time",
                              "sales_amount", F.lit(0).alias("expense_amount")))

exp_norm = (exp_df.select("region_id", "event_time",
                          F.lit(0).alias("sales_amount"), "expense_amount"))

unioned = sales_norm.unionByName(exp_norm)

prof_df = (unioned.withWatermark("event_time", "2 hours")
                    .groupBy(F.window("event_time", "1 hour"), "region_id")
                    .agg(F.sum(F.col("sales_amount")).alias("total_revenue"),
                         F.sum(F.col("expense_amount")).alias("total_expenses"))
                    .withColumn("profit", calculate_profit("total_revenue", "total_expenses"))
                    .withColumn("profit_margin", profit_margin("total_revenue", "profit"))
                    .withColumn("window_start", F.col("window.start"))
                    .withColumn("window_end", F.col("window.end"))
                    .withColumn("date", F.to_date("window_start")))

reg_df = (spark.read
                .table(f"{catalog}.slv.regions")
                .select("region_id", "city"))

joined_df = (prof_df.alias("p")
                    .join(F.broadcast(reg_df).alias("r"),
                            on = ["region_id"], how = "inner")
                    .select("p.date", "p.window_start", "p.window_end", "r.city",
                            "p.total_revenue", "p.total_expenses", "p.profit",
                            "p.profit_margin"))

query = (joined_df.writeStream
            .format("delta")
            .option("checkpointLocation", f"abfss://checkpoints@jayveeradlsdevtest.dfs.core.windows.net/{checkpoints_dir}/gld_checkpoints/reg_hrly_prof_win_checkpoint")
            .outputMode("append")
            .trigger(availableNow = True)
            .table(f"{catalog}.gld.hourly_regional_profit_summary_agg"))

query.awaitTermination()