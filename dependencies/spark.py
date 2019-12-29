"""
spark.py
~~~~~~~
This module contains helper function to use with spark.
"""
import yaml
import __main__

from os import environ, listdir, path
from typing import Optional, List, Dict
from pyspark import SparkFiles
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name: str = 'my_app_name', master: str = 'local[*]', jar_package: Optional[List[str]] = None,
                files: Optional[List[str]] = None,
                spark_config: Dict = None):
    # detect execution environment
    flag_reply = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_reply or flag_debug):
        # get spark session factory
        spark_builder = (
            SparkSession
                .builder
                .appName(app_name)
        )
    else:
        # get spark session factory
        spark_builder = (
            SparkSession
                .builder
                .master(master)
                .appName(app_name)
        )

        # create spark JAR packages
        spark_jars_packages = ','.join(list(jar_package))
        spark_builder.config('spark.jars.packages', spark_jars_packages)

        # create spark Files
        spark_files = ','.join(list(files))
        spark_builder.config('spark.files', spark_files)

        # add other spark config parameters
        for k, v in spark_config.items():
            spark_builder.config(k, v)

    # create spark session and attach spark logger object
    spark_session = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_session)

    # get config file if sent to cluster with ' --files  options'
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename for filename in listdir(spark_files_dir) if filename.endswith('config.yml')]

    if config_files:
        config_file_path = path.join(spark_files_dir, config_files)
        with open(config_file_path, 'r') as config_file:
            config_dict = yaml.full_load(config_file)
        spark_logger.info('loaded configurations from config file: '+config_file[0])
    else:
        spark_logger.warn('No configuration file found...')
        config_dict = None

    return spark_session, spark_logger, config_dict





