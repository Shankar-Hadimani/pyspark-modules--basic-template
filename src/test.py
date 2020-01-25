from dependencies.spark import start_spark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from src.extract_load import ExtractLoadFiles
from src.config_helpers import LoadConfiguration
import sys
import argparse


def main():
    # We only specify the yaml file from arg parse and handle rest
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-f", "--config_file", default="C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\config\\dev"
                                                       "-config.yml", help="Configuration file to load.")
    arg = parser.parse_args()

    print(arg)
    cfg = LoadConfiguration(arg.config_file)
    params = cfg.yaml_loader()
    in_ratings = params['FileSystem']['input']['ratings']
    out_ratings = params['FileSystem']['output']['movies_avg_rating']
    print(in_ratings)
    print(out_ratings)
    spark, log, config = start_spark(app_name='my_etl_job')

    # log that main ETL job is starting
    log.warn('etl_job is up-and-running')
    el = ExtractLoadFiles()

    read_data = el.rdd_extract_data(spark=spark, input_file_path=in_ratings)
    trans_data = transform_data(spark, read_data, get_schema())
    el.load_data(df=trans_data, partitioned=False,
                 write_mode="Overwrite",
                 output_file_path=out_ratings,
                 file_header=True)

    # log the success and terminate Spark application
    log.warn('test_etl_job is finished')
    spark.stop()
    return None


def transform_data(spark, rdd: RDD, schema: StructType) -> DataFrame:
    input_df = rdd.map(lambda x: x.split("::")).map(lambda x: [int(x[0]), int(x[1]), int(x[2]), int(x[3])])
    map_schema = spark.createDataFrame(input_df, schema)
    grp_df = map_schema.groupBy('UserID').agg({'MovieID': 'count', 'Ratings': 'avg'})
    round_df = grp_df \
        .withColumnRenamed('count(MovieID)', 'No of Movies') \
        .withColumn('Average Rating', F.round('avg(Ratings)', 0))
    df = round_df.select('UserID', 'No of Movies', 'Average Rating')
    return df


def get_schema() -> StructType:
    ratings_schema = StructType([
        StructField("UserID", IntegerType(), True),
        StructField("MovieID", IntegerType(), True),
        StructField("Ratings", IntegerType(), True),
        StructField("Timestamp", StringType(), True)
    ])
    return ratings_schema


if __name__ == '__main__':
    main()

