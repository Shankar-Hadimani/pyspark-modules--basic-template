from dependencies.spark import start_spark

spark, log, config = start_spark(
    app_name='etl_test_job',
    files=['config/dev-config.yml']
)

# log the success and terminate Spark application
log.warn('test_etl_job is finished')

df = spark.read.option('delimiter', '|').csv(r'C:\Users\jassm\PycharmProjects\pyspark-asos\resources\input\movies.dat')
df.show()
