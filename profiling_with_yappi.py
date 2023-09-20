# https://github.com/sumerc/yappi/tree/master
import os
import sys
from pathlib import Path

import yappi
from pyspark.sql import SparkSession, DataFrame

from utilities import sum_field


def create_dataframe(spark: SparkSession) -> DataFrame:
    data = [
        (1, 'Sergio', 47),
        (2, 'Carmen', 47),
        (3, 'Jimena', 13),
    ]
    schema = ["id", "name", "age"]
    return spark.createDataFrame(data, schema)


def main():
    spark = SparkSession.builder.getOrCreate()
    print(spark.version)
    print(spark.sparkContext.uiWebUrl)
    df = create_dataframe(spark)
    show_dataframe(df)
    total_age = sum_field(df, "age")
    print(total_age)


def show_dataframe(df):
    df.show(truncate=False)


if __name__ == '__main__':
    yappi.set_clock_type("wall")
    yappi.start()

    main()

    yappi.stop()

    current_module = sys.modules[__name__]
    sum_field_module = sys.modules[sum_field.__module__]
    stats = yappi.get_func_stats(
        filter_callback=lambda stat: yappi.module_matches(stat, [current_module, sum_field_module]))
    sorted_stats = stats.sort("ttot", "desc")
    path = Path(__file__).with_suffix(".txt")
    # https://github.com/sumerc/yappi/blob/master/doc/api.md#savepath-typeystat
    # sorted_stats.save(str(path))

    with open(path, "w") as sys.stdout:
        sorted_stats.print_all(out=sys.stdout)

    sorted_stats.print_all()

# Clock type: WALL
# Ordered by: ttot, desc
#
# name                                  ncall  tsub      ttot      tavg
# ..cts\SparkProfiling\main.py:18 main  1      0.000036  12.32839  12.32839
# ..rofiling\main.py:28 show_dataframe  1      0.000006  3.603755  3.603755
# ..ing\main.py:32 calculate_total_age  1      0.000028  3.138661  3.138661
# ..ofiling\main.py:8 create_dataframe  1      0.000009  2.197639  2.197639
