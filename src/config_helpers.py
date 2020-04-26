import yaml


class LoadConfiguration:

    def __init__(self, file_path):
        self.file_path = file_path

    def yaml_loader(self):
        """ Loads the yaml file to set project parameters """
        with open(self.file_path, "r") as fd:
            data = yaml.full_load(fd)
        return data

    def yaml_dump(self, data):
        """ Dumps the data into yaml file """
        with open(self.file_path, "w") as fd:
            yaml.dump(data, fd)

    def __repr__(self):
        """Returns representation of the object"""
        return "{}({!r})".format(self.__class__, self.file_path)

    def __str__(self):
        return {'File Path': self.file_path}
