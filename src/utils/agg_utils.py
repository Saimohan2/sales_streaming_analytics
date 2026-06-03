from pyspark.sql import functions as F

def calculate_profit(revenue, expenses):

    profit = F.col(revenue) -F.col(expenses)

    return profit

def profit_margin(revenue, profit):

    prof_margin = (F.when(F.col(revenue) == 0, F.lit(0))
                        .otherwise(F.round(F.lit(100.0) * F.col(profit)/F.col(revenue), 2)))
    
    return prof_margin