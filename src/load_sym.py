from pyspark.sql import SparkSession
from pyspark.sql.types import *

sch = StructType([
    StructField("nasdaqTraded", StringType()),
    StructField("symbol", StringType()),
    StructField("securityName", StringType()),
    StructField("listingExchange", StringType()),
    StructField("marketCategory", StringType()),
    StructField("etf", StringType()),
    StructField("roundLotSize", DoubleType()),
    StructField("testIssue", StringType()),
    StructField("financialStatus", StringType()),
    StructField("cqsSymbol", StringType()),
    StructField("nasdaqSymbol", StringType()),
    StructField("nextShares", StringType())
])

def datastore():
    global spark

    spark = SparkSession \
            .builder \
            .appName("spark etl") \
            .master("local[*]") \
            .getOrCreate()

    # get DataFrameReader
    fpath = "datafiles/symbols_valid_meta.csv"
    df = spark.read.csv(fpath, schema=sch, header=True)

    df.show()

    spark.stop()

if __name__ == '__main__':
    datastore()