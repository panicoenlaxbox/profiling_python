# https://github.com/joerick/pyinstrument
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
    main()

# pyinstrument --color .\profiling_with_pyinstrument.py

# 12.819 <module>  profiling_with_pyinstrument.py:1
# └─ 12.734 main  profiling_with_pyinstrument.py:16
#    ├─ 3.976 show_dataframe  profiling_with_pyinstrument.py:26
#    │  └─ 3.976 DataFrame.show  pyspark\sql\dataframe.py:443
#    │        [6 frames hidden]  pyspark, py4j, socket, <built-in>
#    │           3.976 socket.recv_into  <built-in>
#    ├─ 3.394 Builder.getOrCreate  pyspark\sql\session.py:190
#    │     [13 frames hidden]  pyspark, <built-in>, py4j, socket
#    ├─ 3.054 sum_field  utilities.py:4
#    │  └─ 3.004 DataFrame.first  pyspark\sql\dataframe.py:1607
#    │        [10 frames hidden]  pyspark, py4j, socket, <built-in>
#    │           2.980 socket.recv_into  <built-in>
#    └─ 2.309 create_dataframe  profiling_with_pyinstrument.py:6
#       └─ 2.309 SparkSession.createDataFrame  pyspark\sql\session.py:555
#             [16 frames hidden]  pyspark, py4j, socket, <built-in>
#
# To view this report with different options, run:
#     pyinstrument --load-prev 2023-09-20T15-22-55 [options]

# pyinstrument --load-prev 2023-09-20T15-22-55 --renderer html
# pyinstrument --load-prev 2023-09-20T15-22-55 --renderer speedscope --outfile profiling_with_pyinstrument.json
#   https://calmcode.io/pyinstrument/speedscope.html
#   https://www.speedscope.app/
