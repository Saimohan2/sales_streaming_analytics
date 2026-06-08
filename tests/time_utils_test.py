from src.utils.time_utils import date_day_weekOfMonth, get_week_start, add_week_start_date, last_week_start
import datetime

def test_date_day_weekOfMonth(spark):

    data = [(datetime.datetime(2026, 5, 10, 9, 15, 0),),
            (datetime.datetime(2026, 6, 9, 9, 15, 0),)]
    
    schema = ["event_time"]

    df = spark.createDataFrame(data, schema)

    transform_df = date_day_weekOfMonth(spark, df, "event_time")

    results = transform_df.collect()

    assert str(results[0]["event_date"]) == "2026-05-10"
    assert results[1]["day"] == "Tuesday"
    assert results[0]["week_of_month"] == 2

# def test_week_start_ts(spark):

#     data =  [
#         ((datetime.datetime(2026, 6, 9, 0, 0 ,0),),
#          (datetime.datetime(2026, 6, 8, 0, 0 ,0),))
#     ]

#     schema = ["event_time"]

#     df = spark.createDataFrame(data, schema)

#     transform_df = df.withColumn("week_start_ts", get_week_start())

#     results = transform_df.select("week_start_ts").collect()

#     assert results[0]["week_start_ts"] == datetime.datetime(2026, 6, 8, 0, 0, 0)

def test_last_week_start(spark):

    data = [
        ((datetime.datetime(2026, 6, 9, 0, 0 ,0),),
         (datetime.datetime(2026, 6, 8, 0, 0 ,0),))
    ]

    schema = ["event_time"]

    df = spark.createDataFrame(data, schema)

    transform_df = last_week_start(df)

    results = transform_df.select("last_week_start_date").collect()

    print(results)

    assert results[0]["last_week_start_date"] == datetime.date(2026, 6, 1)