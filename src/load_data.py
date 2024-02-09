from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

file_schema = StructType([
    StructField('Date', DateType()),
    StructField('Open', DoubleType()),
    StructField('High', DoubleType()),
    StructField('Low', DoubleType()),
    StructField('Close', DoubleType()),
    StructField('Adj Close', DoubleType()),
    StructField('Volume', LongType())
])

# start spark session
spark = SparkSession \
    .builder \
    .appName("load file") \
    .master("local[*]") \
    .getOrCreate()

print(spark.version)

class Readfile():

    def load_file(self, symbol:str) -> DataFrame:
        fpath = f'datafiles/{symbol}.csv'

        # create a DataFrameReader
        reader = spark.read

        # reading file with explict schema
        df = reader.options(header=True).csv(fpath, schema=file_schema, inferSchema=False)

        # project few columns
        df2 = df.select(df['Date'].alias('date'),
                        df['Open'].alias('open'),
                        df['Close'].alias('close'),
                        df['High'].alias('high'),
                        df['Low'].alias('low')
                        )

        # adding a new column
        dt_str = (df2['date'] + 10).cast(StringType())
        open_str = round(df2['open'], 0).cast(StringType())
        # df3 = df2.withColumn('useless', new_col)
        new_col = concat(dt_str, lit(' hello world '), open_str).alias('useless')
        df3 = df2.select('*', new_col)

        # df for SQL
        df3.createOrReplaceTempView("stock_info")

        query = 'select date,'\
                ' date_add(date, 10) as date_10,'\
                f"concat(cast(date_10 as string), 'hello world' ) as new_col"\
                ' from stock_info'

        spark.sql('select * from stock_info').show(3, truncate=False)
        spark.sql(query).show(3, truncate=False)
        return df3


if __name__ == '__main__':
    cls = Readfile()
    df = cls.load_file('ZYNE')
    df.printSchema()
    # df.show(5, False)
    # df.sort(df['date'].asc()).show(5)
    df.sort(df['date'].desc() , df['open'].asc()).show(5)

    # df.groupby(year(df['date']).alias('year')).max('close').sort('year').show(5)
    df.groupby(year(df['date']).alias('Year')) \
        .agg(min('close'), max('close')) \
        .sort('Year').show(5)
    spark.stop()
