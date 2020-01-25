from src.jobs.top_hundred_movies import transform_data
from src.schemas import get_movies_schema, get_ratings_schema
from tests.test_spark_job import PySparkTest


class TopHundredMoviesTest(PySparkTest):
    raw_movies_data_lst = ["1::Toy Story (1995)::Animation|Children's|Comedy",
                           "2::Jumanji (1995)::Adventure|Children's|Fantasy",
                           "3::Grumpier Old Men (1995)::Comedy|Romance",
                           "4::Waiting to Exhale (1995)::Comedy|Drama",
                           "5::Father of the Bride Part II (1995)::Comedy"]

    raw_ratings_lst = ["1::1::5::978300760",
                       "1::1::3::978302109",
                       "1::1::3::978301968",
                       "1::2::4::978300275",
                       "1::2::5::978824291"]

    def test_row_count_data(self,
                            in_rt_lst=raw_ratings_lst,
                            in_mv_list=raw_movies_data_lst):
        src_mv_data = self.spark.sparkContext.parallelize(in_mv_list)
        src_rt_data = self.spark.sparkContext.parallelize(in_rt_lst)
        actual_df = transform_data(self.spark, src_rt_data, src_mv_data,
                                   get_ratings_schema(), get_movies_schema())
        self.assertEqual(actual_df.count(), 2)

    def test_col_count_data(self,
                            in_rt_lst=raw_ratings_lst,
                            in_mv_list=raw_movies_data_lst):
        src_mv_data = self.spark.sparkContext.parallelize(in_mv_list)
        src_rt_data = self.spark.sparkContext.parallelize(in_rt_lst)
        actual_df = transform_data(self.spark, src_rt_data, src_mv_data, get_ratings_schema(), get_movies_schema())
        self.assertEqual(len(actual_df.columns), 4)

    def test_validate_data(self,
                           in_rt_lst=raw_ratings_lst,
                           in_mv_list=raw_movies_data_lst):
        src_mv_data = self.spark.sparkContext.parallelize(in_mv_list)
        src_rt_data = self.spark.sparkContext.parallelize(in_rt_lst)
        actual_df = transform_data(self.spark, src_rt_data, src_mv_data, get_ratings_schema(), get_movies_schema()) \
            .orderBy('dense_rank') \
            .collect()

        expected_df = self.spark.createDataFrame(
            [(2, 'Jumanji (1995)', 5.0, 1), (1, 'Toy Story (1995)', 4.0, 2),
             ], ['MovieID', 'Title', 'avgRatings', 'dense_rank']) \
            .orderBy('dense_rank').collect()
        self.assertEqual(actual_df, expected_df)
