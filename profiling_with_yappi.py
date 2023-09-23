# https://github.com/sumerc/yappi/tree/master
import sys
from pathlib import Path
from types import ModuleType

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


def _is_own_module(m: ModuleType, base_path: str) -> bool:
    return hasattr(m, "__file__") and m.__file__.startswith(base_path)


if __name__ == '__main__':
    yappi.set_clock_type("wall")
    yappi.start()

    main()

    yappi.stop()

    # current_module = sys.modules[__name__]
    # sum_field_module = sys.modules[sum_field.__module__]
    # modules = [current_module, sum_field_module]

    parent_path = str(Path(__file__).parent)
    modules = [m for m in sys.modules.values() if hasattr(m, "__file__") and m.__file__.startswith(parent_path)]

    stats = yappi.get_func_stats(
        filter_callback=lambda stat: yappi.module_matches(stat, modules))
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
# ..ng\profiling_with_yappi.py:22 main  1      0.000061  14.31176  14.31176
# ..ng_with_yappi.py:32 show_dataframe  1      0.000006  4.266180  4.266180
# ..Profiling\utilities.py:4 sum_field  1      0.000024  3.435901  3.435901
# .._with_yappi.py:12 create_dataframe  1      0.000011  2.633379  2.633379
