from dependencies.spark import start_spark
from pyspark.rdd import RDD
from pyspark.sql import DataFrame, functions as func
from pyspark.sql.types import StructType
from src.config_helpers import LoadConfiguration
from src.extract_load import ExtractLoadFiles
from src.schemas import get_ratings_schema


def transform_data(spark, rdd: RDD, schema: StructType) -> DataFrame:
    input_df = rdd.map(lambda x: x.split("::")).map(lambda x: [int(x[0]), int(x[1]), int(x[2]), int(x[3])])
    map_schema = spark.createDataFrame(input_df, schema)
    grp_df = map_schema.groupBy('UserID').agg({'MovieID': 'count', 'Ratings': 'avg'})
    round_df = grp_df \
        .withColumnRenamed('count(MovieID)', 'No of Movies') \
        .withColumn('Average Rating', func.round('avg(Ratings)', 0))
    df = round_df.select('UserID', 'No of Movies', 'Average Rating')
    return df


class MovieAvgRating:

    def __init__(self):
        self.app_name = "Movies Average Ratings"

    def run(self, param_file):
        # specify the yaml file from arg parse and handle rest
        cfg = LoadConfiguration(param_file)
        params = cfg.yaml_loader()

        project_home = params['project_home_dir']
        in_ratings = project_home + params['FileSystem']['input']['ratings']
        out_ratings = project_home + params['FileSystem']['output']['movies_avg_rating']

        spark, log, config = start_spark(app_name=self.app_name)

        # log that main ETL job is starting
        log.warn('job is up-and-running')

        # create an extract load object to read and write files
        el = ExtractLoadFiles()

        read_data = el.rdd_extract_data(spark=spark, input_file_path=in_ratings)
        trans_data = transform_data(spark, read_data, get_ratings_schema())
        el.load_data(df=trans_data, partitioned=False, write_mode="Overwrite", output_file_path=out_ratings,
                     file_header=True)

        # log the success and terminate Spark application
        log.warn('job is finished')
        spark.stop()
        return None
