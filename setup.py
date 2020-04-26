from distutils.core import setup

setup(
    name='pyspark-asos',
    version='1.0',
    packages=['dependencies', 'py4j', 'pyspark', 'pyspark.ml', 'pyspark.ml.param', 'pyspark.ml.linalg', 'pyspark.sql',
              'pyspark.mllib', 'pyspark.mllib.stat', 'pyspark.mllib.linalg', 'pyspark.streaming'],
    url='',
    license='',
    author='jassm',
    author_email='shankar.hadimani@gmail.com',
    description='setup pyspark project', requires=['pandas']
)
