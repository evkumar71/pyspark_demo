from pyspark.sql import SparkSession, DataFrameWriter, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

meta_sch = StructType([
    StructField("Nasdaq Traded", StringType()),
    StructField("Symbol", StringType()),
    StructField("Security Name", StringType()),
    StructField("listing Exchange", StringType()),
    StructField("Market Category", StringType()),
    StructField("ETF", StringType()),
    StructField("Round Lot Size", DoubleType()),
    StructField("Test Issue", StringType()),
    StructField("Financial Status", StringType()),
    StructField("CQS Symbol", StringType()),
    StructField("NASDAQ Symbol", StringType()),
    StructField("NextShares", StringType())
])

sym_sch = StructType([
    StructField('Date', DateType()),
    StructField('Open', DoubleType()),
    StructField('High', DoubleType()),
    StructField('Low', DoubleType()),
    StructField('Close', DoubleType()),
    StructField('Adj Close', DoubleType()),
    StructField('Volume', LongType())
])

symbols = ['ZUO', 'ZVO', 'ZYME', 'ZYNE', 'ZYXI']

def datastore():
    global spark

    spark = SparkSession \
            .builder \
            .appName("spark etl") \
            .master("local[*]") \
            .getOrCreate()

    # DataFrameReader
    csv_path = "datafiles/symbols_valid_meta.csv"
    df = spark.read.csv(csv_path, schema=meta_sch, header=True)
    df.show(10)

    # DataFrameWriter
    pq_path = "derived/symbols.parquet"
    df_writer = DataFrameWriter(df)
    df_writer.parquet(path=pq_path, mode="overwrite")

    for sym in symbols:
        find_max(sym)

    spark.stop()

def find_max(sym: str):
    csv_path = f"datafiles/{sym}.csv"

    df = spark.read.csv(csv_path, schema=sym_sch, header=True)
    df2 = df.withColumn('sym', lit(sym))
    df2.groupby(df2['sym'], year(df2['Date']).alias('year')) \
        .agg(min('Close').alias('minClose'), max('Close').alias('maxClose')) \
        .orderBy(col('year')) \
        .show(5)

    # df.select(min(df['Close']).alias(f"{sym} Min Close"), max(df['Close']).alias(f"{sym} Max Close")).show()

if __name__ == '__main__':
    datastore()