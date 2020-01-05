from dependencies.spark import start_spark
from src.extract_load import ExtractLoadFiles
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class MoviesAvgRating:
    app_name = "Movies Average Rating By Users"
    output_dir_name = "average_movies"

    def run(self):
        # start Spark application and get Spark session, logger and config
        spark, log, config = start_spark(
            app_name=MoviesAvgRating.app_name,
            files=['configs/dev-config.json'])

        log.info("Spark Session initiated for application : " + MoviesAvgRating.app_name)

        # get all the parameters
        print(config)

        conf = spark.sparkContext._conf.getAll()
        print(conf)

        # instantiate the object
        etl = ExtractLoadFiles(r"C:\Users\jassm\PycharmProjects\pyspark-asos\resources\input\ratings.dat",
                               r"C:\Users\jassm\PycharmProjects\pyspark-asos\resources\output"
                               + "/" + MoviesAvgRating.output_dir_name,
                               col_delimiter="::", file_type="csv", file_header=None)

        # execute ETL pipeline
        data = etl.extract_data(spark)
        transform = transform_data(data)
        etl.load_data(transform, partitioned=False, write_mode="Overwrite")
        return None


def transform_data(df: DataFrame) -> DataFrame:
    read_df = df.withColumn("NEW_COLUMN_TEST", lit('-0000'))
    return read_df


if __name__ == '__main__':
    a = MoviesAvgRating()
    a.run()
