from importlib import import_module
import argparse
import logging


def run_spark_jobs(module_name, class_name, param_file):
    """
    Return a class instance from a string reference
    :param module_name: The fully qualified  module name
    :param class_name: The class name
    :param param_file: configuration file path
    :return:
    """
    print(module_name)
    print(class_name)
    print(param_file)

    try:
        module_ = import_module(module_name)
        try:
            class_ = getattr(module_, class_name)().run(param_file)
        except AttributeError:
            logging.error('Class does not exist')
    except ImportError:
        logging.error('Module does not exist')
    return class_ or None


if __name__ == '__main__':

    # specify the yaml file from arg parse and handle rest
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-m", "--module_name",
                        default="src.jobs.movies_under_genre", help="Spark job module name to be run")

    parser.add_argument("-c", "--class_name",
                        default="MovieUnderRating", help="Class name from the module to instantiated")

    parser.add_argument("-config", "--config_file",
                        default="C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\config\\dev"
                                "-config.yml", help="project configuration file for parameters other than environment "
                                                    "variables")
    arg = parser.parse_args()
    run_spark_jobs(arg.module_name, arg.class_name, arg.config_file)
