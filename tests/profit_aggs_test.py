from src.utils.agg_utils import calculate_profit, profit_margin

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

def test_profit_margin(spark):

    data = [
        (2000, 1000),
        (1000, -500)
    ]

    schema = ["total_revenue", "profit"]

    df = spark.createDataFrame(data, schema)

    profit_margin_df = df.withColumn("profit_margin", profit_margin("total_revenue", "profit"))

    results = profit_margin_df.select("profit_margin").collect()

    assert results[0]["profit_margin"] == 50.00
    assert results[1]["profit_margin"] == -50.00