import json
import unittest

from dependencies.spark import start_spark


class PySparkTest(unittest.TestCase):
    def setUp(self):
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark()

    def tearDown(self) -> None:
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
