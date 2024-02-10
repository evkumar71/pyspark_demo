from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import datetime

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

    def load_file(self, symbol: str) -> DataFrame:
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

        query = 'select date,' \
                ' date_add(date, 10) as date_10,' \
                f"concat(cast(date_10 as string), 'hello world' ) as new_col" \
                ' from stock_info'

        spark.sql('select * from stock_info').show(3, truncate=False)
        spark.sql(query).show(3, truncate=False)

        # define window fn-ex1
        win = Window.partitionBy(year(df3['date']), month(df3['date'])) \
            .orderBy(df3['close'].desc(), df3['date'])

        # apply window-fn on the win
        df3.withColumn('rank', row_number().over(win)) \
            .filter(col('rank') == 1) \
            .show(10)

        # window fn-ex2 - partition entire dataset
        win = Window.partitionBy().orderBy(df3['date'])

        df3.withColumn('prevDayClose', lag(df3['close'], 1, 0.0).over((win))) \
            .withColumn('diffPrevClose', df3['open'] - col('prevDayClose')) \
            .withColumn('diffPrevCloseNew',
                        when(col('prevDayClose') == 0.0, 0).otherwise(df3['open'] - col('prevDayClose'))) \
            .show()

        return df3

    def df_test(self):
        schema = StructType() \
            .add('date', DateType(), True) \
            .add('open', DoubleType(), True) \
            .add('close', DoubleType(), True)

        df = spark.createDataFrame([
            Row(date=None, open=None, close=2.0),
            Row(date=datetime.date(2024, 1, 31), open=1.0, close=2.0),
            Row(date=datetime.date(2024, 1, 31), open=2.0, close=3.0),
            Row(date=datetime.date(2024, 1, 31), open=3.0, close=None)
        ], schema)

        df.show()
        default_dt = datetime.date(9999, 12, 31)
        df.na.fill({'open': 0.0, 'close': 1.0})

        df.withColumn('date', when(df['date'].isNull(), default_dt) \
                      .otherwise((df['date']))).show()


if __name__ == '__main__':
    cls = Readfile()
    cls.df_test()

    df = cls.load_file('ZYNE')
    df.printSchema()
    df.sort(df['date'].asc()).show(5)
    df.sort(df['date'].desc(), df['open'].asc()).show(5)

    df.groupby(year(df['date']).alias('Year'), month(df['date']).alias('Month')) \
        .agg(max('close').alias('maxClose'), avg('close').alias('avgClose'), sum('open').alias('sumOpen')) \
        .sort(col('maxClose').desc()) \
        .show()

    spark.stop()
