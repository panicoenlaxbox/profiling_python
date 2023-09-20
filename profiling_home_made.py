from functools import wraps
from time import perf_counter
from pyspark.sql import SparkSession, DataFrame
from utilities import sum_field
import inspect


# The timer is ready to receive parameters if they are needed.
# Simply add them.
# That is why it is used with parentheses, @timer(), instead of timer, without parentheses.
def timer():
    def _outer(fn):
        @wraps(fn)
        def _inner(*args, **kwargs):
            tic = perf_counter()
            value = fn(*args, **kwargs)
            toc = perf_counter()
            elapsed_time = toc - tic
            print(f"{fn.__module__}.{fn.__name__} at {inspect.getfile(fn)} took {elapsed_time:.6f} seconds")
            return value

        return _inner

    return _outer


class Timer:
    def __init__(self, message=None):
        self.start = perf_counter()
        self.message = message or "Code block"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        end = perf_counter()
        elapsed_time = end - self.start
        print(f"{self.message} took {elapsed_time:.6f} seconds")


def create_dataframe(spark: SparkSession) -> DataFrame:
    data = [
        (1, 'Sergio', 47),
        (2, 'Carmen', 47),
        (3, 'Jimena', 13),
    ]
    schema = ["id", "name", "age"]
    return spark.createDataFrame(data, schema)


@timer()
def main():
    spark = SparkSession.builder.getOrCreate()
    print(spark.version)
    print(spark.sparkContext.uiWebUrl)
    df = create_dataframe(spark)
    with Timer():
        show_dataframe(df)
    with Timer("calculating_age"):
        total_age = sum_field(df, "age")
    print(total_age)


def show_dataframe(df):
    show_non_truncated_dataframe(df)


@timer()
def show_non_truncated_dataframe(df):
    df.show(truncate=False)


if __name__ == '__main__':
    main()

# __main__.show_non_truncated_dataframe at profiling_hand_made.py took 3.752711 seconds
# Code block took 3.752752 seconds
# calculating_age took 2.917539 seconds
# __main__.main at profiling_hand_made.py took 12.330486 seconds

