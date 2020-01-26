import argparse
import importlib
import logging
import os


def run_spark_jobs(class_name, param_file):
    """
    Return a class instance from a string reference
    :param class_name: The absolute class name
    :param param_file: configuration file path
    :return:
    """
    # split module and individual class names
    module_name, class_name = class_name.rsplit('.', 1)
    try:
        module_ = importlib.import_module(module_name)
        try:
            class_ = getattr(module_, class_name)().run(param_file)
        except AttributeError:
            logging.error('Class does not exist')
    except ImportError:
        logging.error('Module does not exist')
    return class_ or None


if __name__ == '__main__':

    # specify the yaml filename from arg parse and handle rest
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("-class", "--class_name",
                        default="src.jobs.movies_under_genre.MovieUnderRating",
                        help="absolute class name - module.submodules.class")

    # get configuration file path environment variable
    try:
        config_file = os.environ['CONFIG_FILE_PATH']
        parser.add_argument("-config", "--config_file", default=config_file,
                            help="project configuration/parameters file, other than environment variables")
    except KeyError as e:
        print("Cannot find config path in environment variables, so using default unless passed as an argument with "
              "option --config : " + str(e))

        parser.add_argument("-config", "--config_file",
                            default="C:\\Users\\jassm\\PycharmProjects\\pyspark-asos\\config\\dev"
                                    "-config.yml", help="project configuration/parameters file, other than environment "
                                                        "variables")
    arg = parser.parse_args()
    run_spark_jobs(arg.class_name, arg.config_file)
