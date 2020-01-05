# implementing duck typing with main clause
from pyspark.sql import DataFrame


class RunSparkJob:

    def run_spark_job(self, etl):
        etl.run()


if __name__ == '__main__':
    etl = TestJob()
    a = RunSparkJob()
    a.run_spark_job(etl)

