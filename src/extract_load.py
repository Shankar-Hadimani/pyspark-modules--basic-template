from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class ExtractLoad(ABC):

    @abstractmethod
    def extract_data(self, spark, input_file_path: str = None, col_delimiter=",",
                     file_type="csv", file_header=None): pass

    @abstractmethod
    def load_data(self, df: DataFrame, partitioned=True, write_mode="Overwrite",
                  output_file_path: str = None, col_delimiter=",",
                  file_type="csv", file_header=None): pass

    @abstractmethod
    def rdd_extract_data(self, spark, input_file_path): pass


class ExtractLoadFiles(ExtractLoad):

    def extract_data(self, spark, input_file_path: str = None, col_delimiter=",",
                     file_type="csv", file_header=None) -> DataFrame:
        if file_type.upper() == "CSV":
            read_df = (
                spark.read.format("csv")
                    .option("delimiter", col_delimiter)
                    .option("header", file_header)
                    .load(input_file_path)
            )
        elif file_type == "parquet":
            read_df = (
                spark.read.parquet(input_file_path).option("header", file_header)
            )
        elif file_type == "json":
            read_df = (
                spark.read.json(input_file_path)
            )
        else:
            raise ValueError(
                "Un-recognised file formats.Supported file formats are csv, json and parquet. Cannot read the input "
                "file: " + input_file_path)
        return read_df

    def load_data(self, df: DataFrame, partitioned=True, write_mode="Overwrite",
                  output_file_path: str = None, col_delimiter=",",
                  file_type="csv", file_header=None):
        if file_type.upper() == "CSV":
            if partitioned:
                df.write.mode(write_mode).option("header", file_header).csv(output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", file_header).csv(output_file_path)
        elif file_type.upper() == "PARQUET":
            if partitioned:
                df.write.mode(write_mode).option("header", file_header).parquet(output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", file_header).parquet(output_file_path)
        elif file_type.upper() == "JSON":
            if partitioned:
                df.write.mode(write_mode).option("header", file_header).json(output_file_path)
            else:
                df.coalesce(1).write.mode(write_mode).option("header", file_header).json(output_file_path)
        else:
            raise ValueError(
                "Un-recognised file formats.Supported file formats are csv, json and parquet. Cannot write the output "
                "to: " + output_file_path)
        return None

    def rdd_extract_data(self, spark, input_file_path):
        read_rdd = spark.sparkContext.textFile(input_file_path)
        return read_rdd
