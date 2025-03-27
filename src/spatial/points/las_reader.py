from pathlib import Path
from pyspark.sql import SparkSession
import laspy
import numpy as np
from typing import Generator, Optional, Tuple, Dict
import os

try:
    from pyspark.sql.datasource import DataSource, DataSourceReader
    from pyspark.sql.types import *
except ImportError as e:
    print(f"Error importing PySpark custom data sources: {e}. This feature is only available in Databricks Runtime 15.2 and above.")
    print("PySpark custom data sources are in Public Preview in Databricks Runtime 15.2 and above, and on serverless environment version 2. Streaming support is available in Databricks Runtime 15.3 and above.")
    raise

class LASToGeometryDataSourceReader(DataSourceReader):
    """
    Data source reader to read LAS/LAZ files and convert them to a dataframe.

    Attributes:
        schema (StructType): The schema of the output data, including fields for x, y, z, intensity and headers.
        options (dict): Configuration options to customize the data reader.

    Options:
        - `path` (str): The file path to the input LAS/LAZ file. **Required**.
        - `chunkSize` (int): Process records in chunks of `chunkSize` records at a time. Defaults to 1000000.

    Example Usage:
        df = (
            spark.read.format("las")
            .option("path", path)
            .option("chunkSize", "1000000")
            .load()
        )
    """
    def __init__(self, schema: StructType, options: dict):
        """
        Initialize the LASToGeometryDataSourceReader.

        Args:
            schema (StructType): The schema of the output data.
            options (dict): Options to configure the data reader, such as file path and filters.
        """
        self.schema: StructType = schema
        self.options: dict = options

    def check_directory(directory_path):
        if os.path.isdir(directory_path):
            print(f"Directory exists: {directory_path}")
        else:
            print(f"Directory does not exist: {directory_path}")

    def read(self, partition: Optional[int] = None) -> Generator[Tuple[int, str, Optional[str], Dict[str, str]], None, None]:
        """
        Read the LAS file and yield data for each points.

        Args:
            partition (Optional[int]): Partition index, if applicable. Not implemented.

        Yields:
            tuple: A tuple containing the point's data
        """
        # Extract options
        input_path: str = self.options.get("path")
        if not input_path:
            raise ValueError("The 'path' option is required.")

        chunk_size: int = self.options.get("chunkSize", 1000000)

        # TODO: process files in a directory (for now supports only file)

        with laspy.open(input_path) as f:

            # TODO: test performance of using scaled coords vs calculating them on the fly

            for points in f.chunk_iterator(chunk_size):
                x_float = np.array(points.x).astype(float)
                y_float = np.array(points.y).astype(float)
                z_float = np.array(points.z).astype(float)
                
                # Check if the 'gps_time' field is present
                gps_time = points.gps_time if hasattr(points, 'gps_time') else [None] * len(x_float)
                red = points.red if hasattr(points, 'red') else [None] * len(x_float)
                green = points.green if hasattr(points, 'green') else [None] * len(x_float)
                blue = points.blue if hasattr(points, 'blue') else [None] * len(x_float)

                for point in zip(
                    x_float, y_float, z_float, points.intensity, points.return_number, points.number_of_returns,
                    points.scan_direction_flag, points.edge_of_flight_line, points.classification, points.synthetic,
                    points.key_point, points.withheld, points.scan_angle_rank, points.user_data, points.point_source_id,
                    gps_time, red, green, blue
                ):
                    yield point

class LASToGeometryDataSource(DataSource):
    """
    A custom data source to convert LAS/LAZ files to geometries in WKT, WKB, or GeoJSON format,
    including tags for each object, using MapType for tags.
    """
    @classmethod
    def name(cls) -> str:
        """
        Get the name of the data source.

        Returns:
            str: The name of the data source.
        """
        return "las"

    def schema(self) -> StructType:
        """
        Define the schema for the output data.
        The schema includes fields present in point format 3.

        Returns:
            StructType: The schema including these fields:
            - x: scaled x coordinate
            - y: scaled y coordinate
            - z: scaled z coordinate 
            - intensity: the integer representation of the pulse return magnitude
            - return_number: is the pulse return number for a given output pulse
            - number_of_returns: total number of returns for a given pulse
            - scan_direction_flag: the direction at which the scanner mirror was traveling at the time of the output pulse
            - edge_of_flight_line: data bit has a value of 1 only when the point is at the end of a scan. It is the last point on a given scan line before it changes direction.
            - classification: point classes
            - synthetic:
            - key_point:
            - withheld:
            - scan_angle_rank: valid range from -90 to +90. The Scan Angle Rank is the angle (rounded to the nearest integer in the absolute value sense) at which the laser point was output from the laser system including the roll of the aircraft.  
            - user_data: additional information about the point
            - point_source_id: This value indicates the file from which this point originated.
            - gps_time:  double floating point time tag value at which the point was acquired
            - red: The Red image channel value associated with this point 
            - green: The Green image channel value associated with this point 
            - blue: The Blue image channel value associated with this point 
        """
        return StructType([
            StructField("x", FloatType(), True),
            StructField("y", FloatType(), True),
            StructField("z", FloatType(), True),
            StructField("intensity", ShortType(), True),
            StructField("return_number", ShortType(), True),
            StructField("number_of_returns", ShortType(), True),
            StructField("scan_direction_flag", ByteType(), True),
            StructField("edge_of_flight_line", ByteType(), True),
            StructField("classification", ByteType(), True),
            StructField("synthetic", ByteType(), True),
            StructField("key_point", ByteType(), True),
            StructField("withheld", ByteType(), True),
            StructField("scan_angle_rank", ByteType(), True),
            StructField("user_data", ByteType(), True),
            StructField("point_source_id", ShortType(), True),
            StructField("gps_time", DoubleType(), True),
            StructField("red", ShortType(), True),
            StructField("green", ShortType(), True),
            StructField("blue", ShortType(), True)
        ])

    def reader(self, schema: StructType) -> LASToGeometryDataSourceReader:
        """
        Create a data source reader for reading the LAS file.

        Args:
            schema (StructType): The schema of the output data.

        Returns:
            LASToGeometryDataSourceReader: An instance of the data source reader.
        """
        return LASToGeometryDataSourceReader(schema, self.options)

def register_las_data_source():
    if os.getenv("IS_SERVERLESS") == "TRUE":
        raise RuntimeError(
            "Error: This data source can only be executed in a non-serverless context. "
            "Please attach the notebook to a traditional compute cluster and try again."
        )
    
    spark = SparkSession.getActiveSession()
    try:
        spark.dataSource.register(LASToGeometryDataSource)
        print("Custom data source 'las' registered successfully.")
    except AttributeError:
        print("Error registering custom data source: PySpark custom data sources are not supported in this environment.")
