from pyspark.rdd import RDD
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, LongType, StringType

from dependencies.spark import start_spark
from src.extract_load import ExtractLoadFiles


class TestJob(ExtractLoadFiles):

    def run(self):
        spark, log, config = start_spark(
            app_name='my_etl_job',
            files=['config/dev_config.yml'])

        # log that main ETL job is starting
        log.warn('etl_job is up-and-running')
        el = ExtractLoadFiles("C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\resources\\input\\ratings.dat",
                              "C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\resources\\output\\test",
                              col_delimiter="|")

        read_data = el.rdd_extract_data(spark=spark)
        trans_data = transform_data(spark, read_data, get_schema())
        el.load_data(trans_data, False)
        # trans_data.show(5)


def transform_data(spark, rdd: RDD, schema: StructType) -> DataFrame:
    input_df = rdd.map(lambda x: x.split("::"))
    map_schema = spark.createDataFrame(input_df, schema)
    df = map_schema.groupBy('UserID').agg({'MovieID': 'count', 'Ratings': 'avg'})
    # df = map_schema.withColumn("NEW_COLUMN_TEST", F.lit('-0000'))
    return df


def get_schema() -> StructType:
    ratings_schema = StructType([
        StructField("UserID", StringType(), True),
        StructField("MovieID", StringType(), True),
        StructField("Ratings", StringType(), True),
        StructField("Timestamp", StringType(), True)
    ])
    return ratings_schema


a = TestJob()
a.run()
