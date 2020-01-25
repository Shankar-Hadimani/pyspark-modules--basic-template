from dependencies.spark import start_spark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, functions as func
from pyspark.sql.types import StructType
from src.config_helpers import LoadConfiguration
from src.extract_load import ExtractLoadFiles
from src.schemas import get_movies_schema


class MovieUnderRating:
    def __init__(self):
        self.app_name = "Number of Movies under each Genres"

    def run(self, param_file):
        # specify the yaml file from arg parse and handle rest
        cfg = LoadConfiguration(param_file)
        params = cfg.yaml_loader()

        project_home = params['project_home_dir']
        in_file = project_home + params['FileSystem']['input']['movies']
        out_file = project_home + params['FileSystem']['output']['movies_under_genre']

        spark, log, config = start_spark(app_name=self.app_name)

        # log that main ETL job is starting
        log.warn('Job is up-and-running')
        el = ExtractLoadFiles()

        read_data = el.rdd_extract_data(spark=spark, input_file_path=in_file)
        trans_data = transform_data(spark, read_data, get_movies_schema())
        el.load_data(df=trans_data, partitioned=False, write_mode="Overwrite", output_file_path=out_file,
                     file_header=True)

        # log the success and terminate Spark application
        log.warn('Job  is finished')
        spark.stop()
        return None


def transform_data(spark, rdd: RDD, schema: StructType) -> DataFrame:
    input_df = rdd.map(lambda x: x.split("::")).map(lambda x: [int(x[0]), str(x[1]), str(x[2])])
    map_schema = spark.createDataFrame(input_df, schema)
    movies = map_schema.withColumn('Genre', func.explode(func.split('Genres', '\\|'))).distinct()
    agg_movies = movies.groupBy('Genre').agg({'Title': 'count'})
    df = agg_movies.withColumnRenamed('count(Title)', 'Total_Movies')
    return df
