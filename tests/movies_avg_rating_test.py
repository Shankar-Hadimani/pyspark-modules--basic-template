from src.jobs.movies_avg_rating import transform_data
from src.schemas import get_ratings_schema
from tests.test_spark_job import PySparkTest


class MoviesAvgRatingTest(PySparkTest):
    raw_data_list = ["1::1::5::978300760",
                     "1::1::3::978302109",
                     "1::1::3::978301968",
                     "1::2::4::978300275",
                     "1::2::5::978824291"]

    def test_row_count_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_ratings_schema())
        self.assertEqual(actual_df.count(), 1)

    def test_col_count_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_ratings_schema())
        self.assertEqual(len(actual_df.columns), 3)

    def test_validate_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_ratings_schema()).orderBy('Average Rating')\
            .collect()

        expected_df = self.spark.createDataFrame([(1, 5, 4.0)], ['UserID', 'No of Movies', 'Average Rating'])\
            .orderBy('Average Rating').collect()
        self.assertEqual(expected_df, actual_df)
