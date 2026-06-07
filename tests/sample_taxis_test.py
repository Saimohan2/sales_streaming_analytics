from src.utils.agg_utils import calculate_profit

def test_profit_func(spark):

    data = [
        (2000, 1000),
        (1000, 1500)
    ]

    schema = ["total_revenue", "total_expenses"]

    df = spark.createDataFrame(data, schema)

    profit_df = df.withColumn("profit", calculate_profit("total_revenue", "total_expenses"))

    results = profit_df.select("profit").collect() 

    assert results[0]["profit"] == 1000
    assert results[1]["profit"] == -500