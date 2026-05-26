from pyspark.sql import functions as F

def parse_json(spark, df, col, schema):

    parsed_df = df.withColumn("parsed", F.from_json(F.col(col), schema))

    return parsed_df