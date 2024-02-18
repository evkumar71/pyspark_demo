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


def get_session():
    ses = SparkSession \
        .builder \
        .appName("spark etl") \
        .master("local[*]") \
        .getOrCreate()

    return ses


def write_parquet(p_df: DataFrame):
    pq_path = "derived/symbols.parquet"
    # DataFrameWriter
    df_writer = DataFrameWriter(p_df)
    df_writer.parquet(path=pq_path, mode="overwrite")


def datastore():
    # DataFrameReader
    csv_path = "datafiles/symbols_valid_meta.csv"
    df = spark.read.csv(csv_path, schema=meta_sch, header=True)

    df2 = df.select(df['Nasdaq Traded'].alias('nasdaqTraded'),
                    df['Symbol'].alias('symbol'),
                    df['Security Name'].alias('securityName'),
                    df['Listing Exchange'].alias('listingExchange'),
                    df['Market Category'].alias('marketCategory'),
                    df['ETF'].alias('etf'),
                    df['Round Lot Size'].alias('roundLotSize'),
                    df['Test Issue'].alias('testIssue'),
                    df['Financial Status'].alias('financialStatus'),
                    df['CQS Symbol'].alias('cqsSymbol'),
                    df['NASDAQ Symbol'].alias('nasdaqSymbol'),
                    df['NextShares'].alias('nextShares')
                    )

    write_parquet(df2)

    for sym in symbols:
        find_max(sym)

    for sym in symbols:
        find_sma(sym)

    return df2


def find_max(sym: str):
    csv_path = f"datafiles/{sym}.csv"
    df = spark.read.csv(csv_path, schema=sym_sch, header=True)

    df2 = df.withColumn('sym', lit(sym))
    df2.groupby(df2['sym'], year(df2['Date']).alias('year')) \
        .agg(min('Close').alias('minClose'), max('Close').alias('maxClose')) \
        .orderBy(col('year')) \
        .show(5)


# Simple Moving Average
def find_sma(sym: str):
    csv_path = f"datafiles/{sym}.csv"
    df = spark.read.csv(csv_path, schema=sym_sch, header=True)

    df.groupby(year(df['Date']).alias('year'), month(df['Date']).alias('month')) \
        .agg(avg('Close').alias('simple moving avg')) \
        .orderBy('year', 'month') \
        .show(5)


if __name__ == '__main__':
    spark = get_session()
    df = datastore()
    df.show(10)
    spark.stop()
