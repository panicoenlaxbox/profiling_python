from pyspark.sql import functions as f, DataFrame


def sum_field(df: DataFrame, field_name: str) -> int:
    return int(df.agg(f.sum(field_name)).first()[0])
