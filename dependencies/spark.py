"""
spark.py
~~~~~~~
This module contains helper function to use with spark.
"""
import json
from os import environ, listdir, path
from typing import Optional, List, Dict

import __main__

from dependencies import logging
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def start_spark(app_name: str = 'my_app_name', master: str = 'local[*]', jar_packages: Optional[List[str]] = None,
                files: Optional[List[str]] = None,
                spark_config: Dict = None):
    """
    Start Spark session, get Spark logger and load config files.

    Start a Spark session on the worker node and register the Spark
    application with the cluster. Note, that only the app_name argument
    will apply when this is called from a script sent to spark-submit.
    All other arguments exist solely for testing the script from within
    an interactive Python console.

    This function also looks for a file ending in 'config.json' that
    can be sent with the Spark job. If it is found, it is opened,
    the contents parsed (assuming it contains valid JSON for the ETL job
    configuration) into a dict of ETL job configuration parameters,
    which are returned as the last element in the tuple returned by
    this function. If the file cannot be found then the return tuple
    only contains the Spark session and Spark logger objects and None
    for config.

    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set (e.g.
    setting `DEBUG=1` as an environment variable as part of a debug
    configuration within an IDE such as Visual Studio Code or PyCharm.
    In this scenario, the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.
    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param jar_packages:  List of Spark JAR package names.
    :param files:  List of files to send to Spark cluster (master and workers).
    :param spark_config:  Dictionary of config key-value pairs.
    :return:  A tuple of references to the Spark session, logger and
        config dict (only if available).
    """
    # detect execution environment
    if spark_config is None:
        spark_config = {}

    flag_reply = not (hasattr(__main__, '__file__'))
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
        spark_jars_packages = ','.join(list(jar_packages))
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

    # # # add spark files if ran in client mode
    # spark_files = ','.join(list(files))
    # for extra_file in spark_files:
    #     spark_session.sparkContext.addPyFile(extra_file)

    # get config file if sent to cluster with ' --files  options'
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = [filename
                    for filename in listdir(spark_files_dir)
                    if filename.endswith('config.json')]

    if config_files:
        config_file_path = path.join(spark_files_dir, config_files[0])
        with open(config_file_path, 'r') as config_file:
            config_dict = json.load(config_file)
        spark_logger.info('loaded configurations from config file: ' + config_files[0])
    else:
        spark_logger.warn('No configuration file found...')
        config_dict = None

    return spark_session, spark_logger, config_dict
