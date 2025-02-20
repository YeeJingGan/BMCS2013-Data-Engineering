"""
Author: Gan Yee Jing, Yeap Jie Shen
"""

import os
from pyspark.sql import SparkSession

class HdfsClient:
    """
        This is a client class for HDFS related services
    """
    def __init__(self, spark_client = None, hdfs_home = r'hdfs://localhost:9000/user/student/'):
        if not spark_client:
            raise Exception("Please provide spark client!")
        
        self.spark_client = spark_client
        self.spark_context = self.spark_client.sparkContext
        self.hdfs_home = hdfs_home

    def list_directory(self, path = ''):
        """
            List contents in the specified directory through the given path
        """
        os.system(f'hdfs dfs -ls {path}')

    def read_file(self, file_format, source_path, **kwargs):
        """
            Read file from HDFS according to the file format and path specified
        """
        if file_format == 'csv':
            return self._read_csv_file(
                source_path = source_path,
                header = kwargs['header'] if kwargs['header'] else True,
                multiline = kwargs['multiline'] if kwargs['multiline'] else True)
        elif file_format == 'parquet':
            return self._read_parquet_file(source_path = source_path, filename = filename)

    def _read_csv_file(self, source_path, header = True, multiline = True):
        """
            Read csv file from path specified
        """
        return self.spark_client.read.format('csv').option('header', header).option('multiline', multiline).load(self.hdfs_home + source_path)

    def _read_parquet_file(self, source_path):
        """
            Read parquet file from path specified
        """
        return self.spark_client.read.format('parquet').load(self.hdfs_home + source_path)

    def write_file(self, dataframe, file_format, destination_path, **kwargs):
        """
            Read file from HDFS according to the file format and path specified
        """
        if file_format == 'csv':
            self._write_csv_file(
                dataframe = dataframe,
                destination_path = destination_path,
                header = kwargs['header'] if kwargs['header'] else True)
        elif file_format == 'parquet':
            self._write_parquet_file(dataframe, destination_path)

    def _write_csv_file(self, dataframe, destination_path, header = True):
        """
            Write csv file to path specified
        """
        dataframe.write.format('csv').option('header', header).save(self.hdfs_home + destination_path)

    def _write_parquet_file(self, dataframe, destination_path):
        """
            Write parquet file to path specified
        """
        dataframe.write.format('parquet').save(self.hdfs_home + destination_path)

