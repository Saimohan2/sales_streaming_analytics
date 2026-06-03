from pyspark.sql import functions as F

def date_day_weekOfMonth(spark, df, event_time):

    df = (df.withColumn("event_date", F.to_date(F.col(event_time), "yyyy-MM-dd"))
          .withColumn("day", F.date_format(F.col("event_date"), "EEEE"))
          .withColumn("week_of_month", F.weekofyear(F.col("event_date"))
                                        - F.weekofyear(F.date_sub(F.col("event_date"),
                                                                  F.dayofmonth(F.col("event_date"))
                                                                  +1))
                                                                  +1)
                                                                  )
    
    return df

def get_week_start():
    
    return F.date_trunc("week", F.current_timestamp())

def add_week_start_date(df):

    return df.withColumn("week_start_date", 
                            F.to_date(get_week_start()))

def last_week_start(df):

    week_start = get_week_start()

    return df.withColumn("last_week_start_date", 
                            F.to_date(
                                F.date_trunc(
                                    "week", 
                                    get_week_start() - F.expr("INTERVAL 7 DAYS"))
                                    )
                                )