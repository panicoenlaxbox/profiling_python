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

# kernprof -lv .\profiling_with_line_profiler.py

# Wrote profile results to profiling_with_line_profiler.py.lprof
# Timer unit: 1e-06 s
#
# Total time: 11.9164 s
# File: .\profiling_with_line_profiler.py
# Function: main at line 17
#
# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#     17                                           @profile
#     18                                           def main():
#     19         1    3361263.0    3e+06     28.2      spark = SparkSession.builder.getOrCreate()
#     20         1        474.5    474.5      0.0      print(spark.version)
#     21         1       1527.3   1527.3      0.0      print(spark.sparkContext.uiWebUrl)
#     22         1    2171237.8    2e+06     18.2      df = create_dataframe(spark)
#     23         1    3524612.8    4e+06     29.6      show_dataframe(df)
#     24         1    2857159.0    3e+06     24.0      total_age = int(df.agg(f.sum("age")).first()[0])
#     25         1        135.8    135.8      0.0      print(total_age)
#
# Total time: 3.52458 s
# File: .\profiling_with_line_profiler.py
# Function: show_dataframe at line 28
#
# Line #      Hits         Time  Per Hit   % Time  Line Contents
# ==============================================================
#     28                                           @profile
#     29                                           def show_dataframe(df):
#     30         1        267.9    267.9      0.0      print("Showing DataFrame...")
#     31         1    3524315.4    4e+06    100.0      _show_non_truncated_dataframe(df)
