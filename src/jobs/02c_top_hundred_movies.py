import argparse

from dependencies.spark import start_spark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as func
from pyspark.sql.types import StructType
from src.config_helpers import LoadConfiguration
from src.extract_load import ExtractLoadFiles
from src.schemas import get_ratings_schema, get_movies_schema


def run():
    app_name = "Movies Average Rating By Users"

    # specify the yaml file from arg parse and handle rest
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-f", "--config_file", default="C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\config\\dev"
                                                       "-config.yml", help="Configuration file to load.")
    arg = parser.parse_args()
    cfg = LoadConfiguration(arg.config_file)
    params = cfg.yaml_loader()

    project_home = params['project_home_dir']
    in_movies = project_home + params['FileSystem']['input']['movies']
    in_ratings = project_home + params['FileSystem']['input']['ratings']
    out_file = project_home + params['FileSystem']['output']['movies_top_hundred']

    spark, log, config = start_spark(app_name=app_name)

    # log that main ETL job is starting
    log.warn('Job is up-and-running')
    el = ExtractLoadFiles()

    ratings = el.rdd_extract_data(spark=spark, input_file_path=in_ratings)
    movies = el.rdd_extract_data(spark=spark, input_file_path=in_movies)

    trans_data = transform_data(spark, ratings, movies, get_ratings_schema(), get_movies_schema())
    el.load_data(df=trans_data, partitioned=False,
                 write_mode="Overwrite",
                 output_file_path=out_file,
                 file_header=True, file_type='parquet')

    # log the success and terminate Spark application
    log.warn('Job is finished')
    spark.stop()
    return None


def transform_data(spark, rdd_ratings: RDD, rdd_movies: RDD,
                   schema_ratings: StructType, schema_movies: StructType) -> DataFrame:
    ratings_df = rdd_ratings.map(lambda x: x.split("::")) \
        .map(lambda x: [int(x[0]), int(x[1]), int(x[2]), int(x[3])])

    movies_df = rdd_movies.map(lambda x: x.split("::")) \
        .map(lambda x: [int(x[0]), str(x[1]), str(x[2])])

    map_ratings_schema = spark.createDataFrame(ratings_df, schema_ratings)
    tr = map_ratings_schema.alias('tr')

    map_movies_schema = spark.createDataFrame(movies_df, schema_movies)
    tm = map_movies_schema.alias('tm')

    join_df = tr.join(tm, tr.MovieID == tm.MovieID, how='inner').drop(tm.MovieID)
    avg_rating_df = join_df.groupBy('MovieID', 'Title') \
        .agg({'Ratings': 'avg'}).withColumnRenamed('avg(Ratings)', 'avgRatings')

    over_category = Window.orderBy(func.desc('avgRatings'))
    rank_df = avg_rating_df.withColumn('dense_rank', func.dense_rank().over(over_category))
    df = rank_df.where(func.col('dense_rank') <= 100)

    return df


if __name__ == '__main__':
    run()
