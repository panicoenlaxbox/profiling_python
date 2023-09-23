# https://github.com/pyutils/line_profiler
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f


def create_dataframe(spark: SparkSession) -> DataFrame:
    data = [
        (1, 'Sergio', 47),
        (2, 'Carmen', 47),
        (3, 'Jimena', 13),
    ]
    schema = ["id", "name", "age"]
    return spark.createDataFrame(data, schema)


# The decorator will be made automatically available on run
@profile
def main():
    spark = SparkSession.builder.getOrCreate()
    print(spark.version)
    print(spark.sparkContext.uiWebUrl)
    df = create_dataframe(spark)
    show_dataframe(df)
    total_age = int(df.agg(f.sum("age")).first()[0])
    print(total_age)


@profile
def show_dataframe(df):
    print("Showing DataFrame...")
    _show_non_truncated_dataframe(df)


def _show_non_truncated_dataframe(df):
    df.show(truncate=False)


def calculate_total_age(df):
    return int(df.agg(f.sum("age")).first()[0])


if __name__ == '__main__':
    main()

# $env:PYSPARK_PYTHON="python"
# kernprof -lv .\profiling_with_line_profiler.py
# kernprof -lv .\profiling_with_line_profiler.py

# Wrote profile results to profiling_with_line_profiler.py.lprof
# Timer unit: 1e-06 s
#
# Total time: 13.663 s
# File: .\profiling_with_line_profiler.py
# Function: main at line 17
#
# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#     17                                           @profile
#     18                                           def main():
#     19         1    3674836.3    4e+06     26.9      spark = SparkSession.builder.getOrCreate()
#     20         1        342.2    342.2      0.0      print(spark.version)
#     21         1       1731.3   1731.3      0.0      print(spark.sparkContext.uiWebUrl)
#     22         1    2596151.7    3e+06     19.0      df = create_dataframe(spark)
#     23         1    4086201.4    4e+06     29.9      show_dataframe(df)
#     24         1    3303583.6    3e+06     24.2      total_age = int(df.agg(f.sum("age")).first()[0])
#     25         1        125.2    125.2      0.0      print(total_age)
#
# Total time: 4.08617 s
# File: .\profiling_with_line_profiler.py
# Function: show_dataframe at line 28
#
# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#     28                                           @profile
#     29                                           def show_dataframe(df):
#     30         1        158.6    158.6      0.0      print("Showing DataFrame...")
#     31         1    4086010.2    4e+06    100.0      _show_non_truncated_dataframe(df)
