from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
import sys
import os

spark = DatabricksSession.builder.getOrCreate()

# current_file_path = globals().get("__file__", sys.argv[0])
# file_dir = os.path.dirname(os.path.abspath(current_file_path))
# proj_root = os.path.abspath(os.path.join(file_dir, "..", "..", ".."))

# sys.path.append(proj_root)

catalog = sys.argv[1]

from utils.agg_utils import calculate_profit, profit_margin
from utils.time_utils import add_week_start_date

sales_df = (spark.read
                .table(f"{catalog}.slv.sales")
                .filter((F.col("event_date") >= F.to_date(
                                                    F.date_trunc("week", F.current_timestamp()))) &
                        (F.col("event_date") <= F.current_date())))

exp_df = (spark.read
                .table(f"{catalog}.slv.expenses")
                        .filter((F.col("event_date") >= F.to_date(
                                                    F.date_trunc("week", F.current_timestamp()))) &
                        (F.col("event_date") <= F.current_date())))

sales_norm = (sales_df.select("event_time", "region_id", "sales_amount", 
                              F.lit(0).alias("expense_amount")))

exp_norm = (exp_df.select("event_time", "region_id", F.lit(0).alias("sales_amount"), 
                              "expense_amount"))

unioned = sales_norm.unionByName(exp_norm)

running = (unioned.groupBy("region_id")
                .agg(F.sum("sales_amount").alias("total_revenue"),
                     F.sum("expense_amount").alias("total_expenses"))
                     .withColumn("profit", calculate_profit("total_revenue", "total_expenses"))
                     .withColumn("profit_margin", profit_margin("total_revenue", "profit"))
                     .withColumn("week_start_date", 
                                    F.to_date(F.date_trunc("week", F.current_timestamp()))))

reg_df = spark.read.table(f"{catalog}.slv.regions").select("region_id", "city")

joined = (running.alias("t").join(F.broadcast(reg_df).alias("r"),
                                        on = ["region_id"],
                                        how = "inner")
                            .select("t.week_start_date", "r.city", "t.total_revenue",
                                    "t.total_expenses", "t.profit", "t.profit_margin"))

joined.write\
    .format("delta")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.gld.running_weekly_regional_profit_summary")