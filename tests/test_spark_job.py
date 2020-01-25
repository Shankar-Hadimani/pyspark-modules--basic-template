import unittest

from dependencies.spark import start_spark


class PySparkTest(unittest.TestCase):
    def setUp(self):
        self.spark, *_ = start_spark()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
