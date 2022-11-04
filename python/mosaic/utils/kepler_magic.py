import re

import h3
import pandas as pd
from IPython.core.magic import Magics, cell_magic, magics_class
from keplergl import KeplerGl
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, conv, lower, lit

from mosaic.api.accessors import st_astext, st_aswkt
from mosaic.api.constructors import st_geomfromwkt, st_geomfromwkb
from mosaic.api.functions import st_centroid2D, grid_pointascellid, grid_boundaryaswkb, st_setsrid, st_transform
from mosaic.config import config
from mosaic.utils.kepler_config import mosaic_kepler_config


@magics_class
class MosaicKepler(Magics):

    """
    A magic command for visualizing data in KeplerGl.
    """

    def __init__(self, shell):
      Magics.__init__(self, shell)
      self.bng_crsid = 27700
      self.osgb36_crsid = 27700
      self.wgs84_crsid = 4326

    @staticmethod
    def displayKepler(map_instance, height, width):

        """
        Display Kepler map instance in Jupyter notebook.

        Parameters:
        ------------
        map_instance: KeplerGl
            Kepler map instance
        height: int
            Height of the map
        width: int
            Width of the map

        Returns:
        -----------
        None

        Example:
            displayKepler(map_instance, 600, 800)
        """
        decoded = (
            map_instance._repr_html_()
            .decode("utf-8")
            .replace(".height||400", f".height||{height}")
            .replace(".width||400", f".width||{width}")
        )
        ga_script_redacted = re.sub(
            r"\<script\>\(function\(i,s,o,g,r,a,m\).*?GoogleAnalyticsObject.*?(\<\/script\>)",
            "",
            decoded,
            flags=re.DOTALL,
        )
        async_script_redacted = re.sub(
            r"s\.a\.createElement\(\"script\",\{async.*?\}\),",
            "",
            ga_script_redacted,
            flags=re.DOTALL,
        )
        config.notebook_utils.displayHTML(async_script_redacted)

    @staticmethod
    def get_spark_df(table_name):
        """
        Constructs a Spark DataFrame from table name, Pandas DataFrame or Spark DataFrame.

        Parameters:
        ------------
        table_name: str
            Name of the table

        Returns:
        -----------
        Spark DataFrame

        Example:
            get_spark_df(table_name)
        """

        try:
            table = config.ipython_hook.ev(table_name)
            if isinstance(table, pd.DataFrame):  # pandas dataframe
                data = config.mosaic_spark.createDataFrame(table)
            elif isinstance(table, DataFrame):  # spark dataframe
                data = table
            else:  # table name
                data = config.mosaic_spark.read.table(table)
        except NameError:
            try:
                data = config.mosaic_spark.read.table(table_name)
            except:
                raise Exception(f"Table name reference invalid.")
        return data

    @staticmethod
    def set_centroid(pandas_data, feature_type, feature_name):

        """
        Sets the centroid of the geometry column.

        Parameters:
        ------------
        pandas_data: Pandas DataFrame
            Pandas DataFrame containing the geometry column to be visualized in KeplerGl.
        feature_type: str
            Type of the feature column to be visualized in KeplerGl.
            This can be "h3", "bng" or "geometry".
            "geometry" represents geometry column with CRSID 4326.
            "geometry(bng)" or "geometry(osgb36)" represents geometry column with CRSID 27700.
            "geometry(23456)" represents geometry column with 23456 where 23456 is the EPSG code.
        feature_name: str
            Name of the column containing the geometry to be visualized in KeplerGl.

        Returns:
        -----------
        None

        Example:
            set_centroid(pdf, "h3", "hex_id")
            set_centroid(pdf, "bng", "bng_id")
            set_centroid(pdf, "geometry", "geom")
            set_centroid(pdf, "geometry(bng)", "geom")
            set_centroid(pdf, "geometry(osgb36)", "geom")
            set_centroid(pdf, "geometry(27700)", "geom")
            set_centroid(pdf, "geometry(23456)", "geom")
        """

        tmp_sdf = config.mosaic_spark.createDataFrame(pandas_data.iloc[:1])

        if feature_type == "h3":
            tmp_sdf = tmp_sdf.withColumn(feature_name, grid_boundaryaswkb(feature_name))

        centroid = (
            tmp_sdf.select(st_centroid2D(feature_name))
            .limit(1)
            .collect()[0][0]
        )

        # set to centroid of a geom
        mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[1]
        mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[0]

    @cell_magic
    def mosaic_kepler(self, *args):

        """
        A magic command for visualizing data in KeplerGl.

        Parameters:
        ------------
        args: str
            Arguments passed to the magic command.
            The first argument is the name of the table to be visualized in KeplerGl.
            The second argument is the type of the feature column to be visualized in KeplerGl.
            This can be "h3", "bng" or "geometry".
            "geometry" represents geometry column with CRSID 4326.
            "geometry(bng)" or "geometry(osgb36)" represents geometry column with CRSID 27700.
            "geometry(23456)" represents geometry column with 23456 where 23456 is the EPSG code.

        Returns:
        -----------
        None

        Example:
            %mosaic_kepler table_name geometry_column h3 [limit]
            %mosaic_kepler table_name geometry_column bng [limit]
            %mosaic_kepler table_name geometry_column geometry [limit]
            %mosaic_kepler table_name geometry_column geometry(bng) [limit]
            %mosaic_kepler table_name geometry_column geometry(osgb36) [limit]
            %mosaic_kepler table_name geometry_column geometry(27700) [limit]
            %mosaic_kepler table_name geometry_column geometry(23456) [limit]
        """

        inputs = [
            i
            for i in " ".join(list(args)).replace("\n", " ").replace('"', "").split(" ")
            if len(i) > 0
        ]

        if len(inputs) != 3 and len(inputs) != 4:
            raise Exception(
                "Mosaic Kepler magic requires table name, feature column and feature type all to be provided. "
                + "Limit is optional (default 1000)."
            )

        table_name = inputs[0]
        feature_name = inputs[1]
        feature_type = inputs[2]
        limit_ctn = 1000
        if len(inputs) == 4:
            limit_ctn = int(inputs[3])
        data = self.get_spark_df(table_name)
        feature_col_dt = [dt for dt in data.dtypes if dt[0] == feature_name][0]

        if feature_type == "h3":
            if feature_col_dt[1] == "bigint":
                data = data.withColumn(
                    feature_name, lower(conv(col(feature_name), 10, 16))
                )
        elif feature_type == "bng":
            data = (data
                .withColumn(feature_name, grid_boundaryaswkb(feature_name))
                .withColumn(feature_name, st_geomfromwkb(feature_name))
                .withColumn(
                    feature_name,
                    st_transform(st_setsrid(feature_name, lit(self.bng_crsid)), lit(self.wgs84_crsid))
                )
                .withColumn(feature_name, st_aswkt(feature_name)))
        elif feature_type == "geometry":
            data = data.withColumn(feature_name, st_astext(col(feature_name)))
        elif re.search("^geometry\(.*\)$", feature_type).start() != None:
            crsid = feature_type.replace("geometry(", "").replace(")", "").lower()
            if crsid == "bng" or crsid == "osgb36":
                crsid = self.bng_crsid
            else:
                crsid = int(crsid)
            data = (data
                .withColumn(feature_name, st_geomfromwkt(st_aswkt(feature_name)))
                .withColumn(
                    feature_name,
                    st_transform(st_setsrid(feature_name, lit(crsid)), lit(self.wgs84_crsid))
                )
                .withColumn(feature_name, st_aswkt(feature_name)))
        else:
            raise Exception(f"Unsupported geometry type: {feature_type}.")

        allowed_schema = [
            field.name
            for field in data.schema.fields
            if field.dataType.typeName() in ["string", "long", "integer", "double"]
        ]
        data = data.select(*allowed_schema)
        pandas_data = data.limit(limit_ctn).toPandas()

        self.set_centroid(pandas_data, feature_type, feature_name)

        m1 = KeplerGl(config=mosaic_kepler_config)
        m1.add_data(data=pandas_data, name=table_name)

        self.displayKepler(m1, 800, 1200)
