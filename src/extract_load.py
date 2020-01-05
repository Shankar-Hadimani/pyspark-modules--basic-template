from abc import abstractmethod, ABC

from pyspark.sql import DataFrame


class ExtractLoad(ABC):

    @abstractmethod
    def extract_data(self): pass

    @abstractmethod
    def load_data(self): pass

    @abstractmethod
    def rdd_extract_data(self): pass


class ExtractLoadFiles(ExtractLoad):

    def __init__(self, input_file_path: str = None, output_file_path: str = None, col_delimiter=",", file_type="csv",
                 file_header=None):
        self.col_delimiter = col_delimiter
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.file_type = file_type
        self.file_header = file_header

    def extract_data(self, spark) -> DataFrame:
        if self.file_type.upper() == "CSV":
            read_df = (
                spark.read.format("csv")
                .option("delimiter", self.col_delimiter)
                .option("header", self.file_header)
                .load(self.input_file_path)
            )
        elif self.file_type == "parquet":
            read_df = (
                spark.read.parquet(self.input_file_path).option("header", self.file_header)
            )
        elif self.file_type == "json":
            read_df = (
                spark.read.json(self.input_file_path)
            )
        else:
            raise ValueError(
                "Un-recognised file formats.Supported file formats are csv, json and parquet. Cannot read the input "
                "file: " + self.input_file_path)
            read_df = spark.range(0).drop("id")

        return read_df

    def load_data(self, df: DataFrame, partitioned=True, write_mode="Overwrite"):
        if self.file_type.upper() == "CSV":
            if partitioned:
                df.write.mode(write_mode).option("header", self.file_header).csv(self.output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", self.file_header).csv(self.output_file_path)
        elif self.file_type.upper() == "parquet":
            if partitioned:
                df.write.mode(write_mode).option("header", self.file_header).parquet(self.output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", self.file_header).parquet(self.output_file_path)
        elif self.file_type.upper() == "json":
            if partitioned:
                df.write.mode(write_mode).option("header", self.file_header).json(self.output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", self.file_header).json(self.output_file_path)
        else:
            raise ValueError(
                "Un-recognised file formats.Supported file formats are csv, json and parquet. Cannot write the output "
                "to: " + self.output_file_path)
        return None

    def rdd_extract_data(self, spark):
        read_rdd = spark.sparkContext.textFile(self.input_file_path)
        return read_rdd



