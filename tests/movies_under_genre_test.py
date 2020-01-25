from src.jobs.movies_under_genre import transform_data
from src.schemas import get_movies_schema
from tests.test_spark_job import PySparkTest


class MoviesUnderGenreTest(PySparkTest):
    raw_data_list = ["1::Toy Story (1995)::Animation|Children's|Comedy",
                     "2::Jumanji (1995)::Adventure|Children's|Fantasy",
                     "3::Grumpier Old Men (1995)::Comedy|Romance",
                     "4::Waiting to Exhale (1995)::Comedy|Drama",
                     "5::Father of the Bride Part II (1995)::Comedy"]

    def test_row_count_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_movies_schema())
        self.assertEqual(actual_df.count(), 7)

    def test_col_count_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_movies_schema())
        self.assertEqual(len(actual_df.columns), 2)

    def test_validate_data(self, in_list=raw_data_list):
        src_raw_data = self.spark.sparkContext.parallelize(in_list)
        actual_df = transform_data(self.spark, src_raw_data, get_movies_schema()).orderBy('Genre') \
            .collect()

        expected_df = self.spark.createDataFrame(
            [("Animation", 1), ("Children's", 2), ("Comedy", 4), ("Adventure", 1), ("Fantasy", 1), ("Romance", 1),
             ("Drama", 1)
             ], ['Genre', 'Total_Movies']) \
            .orderBy('Genre').collect()
        self.assertEqual(actual_df, expected_df)
