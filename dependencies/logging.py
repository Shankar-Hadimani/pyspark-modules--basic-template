"""
logging.py
~~~~~~~
This module contains a class that wraps the log4j object. This is
instantiated by active SparkContext, enabling log4j logging for pyspark.
"""


class Log4j:
    """Wrapper class for log4j JVM object.

    :param : spark  - SparkSession object
    """
    def __init__(self, spark):
        # get spark app details with which all messages are prefixed.
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' :- ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """ Log an error message

        :param message: Error message to be logged / written
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log warning message

        :param message: Error message to be warned about
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """ Log information message

        :param message: Error message to be displayed as Info
        :return: None
        """
        self.logger.info(message)
        return None
