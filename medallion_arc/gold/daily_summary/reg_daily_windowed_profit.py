from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
import sys
import os

# --------------------------------------------------
# Project module imports
# --------------------------------------------------

# curr_file_path = globals().get("__file__", sys.argv[0]) # globals is a dictionary which stores info of the files in our project
# curr_dir = os.path.dirname(os.path.abspath(curr_file_path))
# project_root = os.path.abspath(os.path.join(curr_dir, "..", "..", ".."))

# sys.path.append(project_root)

from utils.agg_utils import calculate_profit, profit_margin

# --------------------------------------------------
# initialize spark session
# --------------------------------------------------

spark = DatabricksSession.builder.getOrCreate()

catalog = sys.argv[1]
checkpoints_dir = sys.argv[2]

# --------------------------------------------------
# daily profit streaming aggregation
# --------------------------------------------------

sales_df = spark.readStream.table(f"{catalog}.slv.sales")

exp_df = spark.readStream.table(f"{catalog}.slv.expenses")

sales_norm = sales_df.select("region_id", "event_time", "sales_amount", F.lit(0).alias("expense_amount"))

exp_norm = exp_df.select("region_id", "event_time", F.lit(0).alias("sales_amount"), "expense_amount")

unioned = sales_norm.unionByName(exp_norm)

daily_agg = (unioned.withWatermark("event_time", "12 hours")
                .groupBy(F.window("event_time", "1 day"), "region_id")
                .agg(F.sum("sales_amount").alias("total_revenue"),
                     F.sum("expense_amount").alias("total_expenses"))
                .withColumn("profit", calculate_profit("total_revenue", "total_expenses"))
                .withColumn("profit_margin", profit_margin("total_revenue", "profit"))
                .withColumn("window_start", F.col("window.start"))
                .withColumn("window_end", F.col("window.end"))
                .withColumn("date", F.to_date(F.col("window_start")))
                .drop("window"))

reg_df = (spark.read.table(f"{catalog}.slv.regions")
                .select("region_id", "city"))

joined = (daily_agg.alias("d")
                .join(F.broadcast(reg_df).alias("r"),
                            on = ["region_id"],
                            how = "inner")
                .select("d.date", "d.window_start", "d.window_end", "r.city",
                        "d.total_revenue", "d.total_expenses", "d.profit", "d.profit_margin"))

query = (joined.writeStream
            .format("delta")
            .option("checkpointLocation", f"abfss://checkpoints@jayveeradlsdevtest.dfs.core.windows.net/{checkpoints_dir}/gld_checkpoints/reg_daily_prof_win_checkpoint")
            .outputMode("append")
            .trigger(availableNow = True)
            .table(f"{catalog}.gld.daily_regional_profit_summary_agg"))

query.awaitTermination()