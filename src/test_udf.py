from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit
import datetime


# method #1
def date_diff(date_1: datetime.date, date_2: datetime.date):
    if date_1 is not None and date_2 is not None:
        return (date_1 - date_2).days
    return 0


# method #2
@udf(returnType=IntegerType())
def date_diff2(date_1: datetime.date, date_2: datetime.date):
    if date_1 is not None and date_2 is not None:
        return (date_1 - date_2).days
    return None


spark = SparkSession \
    .builder \
    .appName("udf test") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

schema = StructType() \
    .add('date', DateType(), True) \
    .add('open', DoubleType(), True) \
    .add('close', DoubleType(), True)

df1 = spark.createDataFrame([
    Row(date=None, open=0.0, close=0.0),
    Row(date=datetime.date(2023, 8, 10), open=1.0, close=2.0),
    Row(date=datetime.date(2023, 6, 21), open=2.0, close=3.0),
    Row(date=datetime.date(2023, 5, 9), open=3.0, close=4.0),
], schema)

ref_dt = lit(datetime.date(2023, 1, 1))

# method#1
udf_fn = udf(date_diff, IntegerType()).asNondeterministic()
df1.withColumn("daysBetween", udf_fn(df1['date'], ref_dt)).show()

# method2
df1.withColumn('daysBTW2', date_diff2(df1['date'], ref_dt)).show()

spark.stop()
