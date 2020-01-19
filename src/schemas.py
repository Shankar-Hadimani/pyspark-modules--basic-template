from pyspark.sql.types import StructType, IntegerType, StringType, StructField


def get_ratings_schema() -> StructType:
    ratings_schema = StructType([
        StructField("UserID", IntegerType(), True),
        StructField("MovieID", IntegerType(), True),
        StructField("Ratings", IntegerType(), True),
        StructField("Timestamp", StringType(), True)
    ])
    return ratings_schema


def get_movies_schema() -> StructType:
    movies_schema = StructType([
        StructField("MovieID", IntegerType(), True),
        StructField("Title", StringType(), True),
        StructField("Genres", StringType(), True)
    ])
    return movies_schema
