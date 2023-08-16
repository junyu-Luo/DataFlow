from pyspark.sql import SparkSession
import os

# 本地设置
os.environ["PYSPARK_PYTHON"] = "D://env//anaconda3//python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "D://env//anaconda3//python.exe"

spark = SparkSession \
    .builder \
    .appName("tes") \
    .master('local[*]') \
    .enableHiveSupport() \
    .getOrCreate()

spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

sc = spark.sparkContext
# sc.setLogLevel('INFO')
sc.setLogLevel('ERROR')

spark_df = spark.createDataFrame([
    ({"1": 1}, {"1": "1"}, "1", '20220606'),
    ({"2": 2}, {"2": "2"}, "2", '20220606'),
    ({"3": 3}, {"3": "3"}, "3", '20220606')
],
    ['a', 'b', 'c', 'dt']
)

spark_df.show()


def process_func(partitions):
    res = []
    for row in partitions:
        print(row.asDict())
        print(row['dt'])
        res.append(row['dt'])
    yield res


data = spark_df.rdd.repartition(4).mapPartitions(lambda x: process_func(x)).collect()

print(data)
