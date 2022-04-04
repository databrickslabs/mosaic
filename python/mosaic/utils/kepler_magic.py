import re

import h3
from IPython.core.magic import Magics, cell_magic, magics_class
from keplergl import KeplerGl
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, conv, lower
from pyspark.sql.utils import AnalysisException, ParseException

from mosaic.api.accessors import st_astext
from mosaic.api.functions import st_centroid2D
from mosaic.config import config
from mosaic.resources import mosaic_logo_b64str
from mosaic.utils.kepler_config import mosaic_kepler_config


@magics_class
class MosaicKepler(Magics):
    @staticmethod
    def displayKepler(map_instance, height, width):
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
        config.display_handler.display_html(async_script_redacted)

    @staticmethod
    def get_spark_df(table_name):
        try:
            data = config.mosaic_spark.read.table(table_name)  # if spark dataframe
        except (AnalysisException, ParseException):
            try:
                eval_df = eval(table_name)  # if pandas dataframe
                if isinstance(eval_df, DataFrame):
                    data = eval_df
                else:
                    data = config.mosaic_spark.createDataFrame(eval_df)
            except (NameError, TypeError):
                raise Exception(f"Table name reference invalid.")
        return data

    @staticmethod
    def set_centroid(pandas_data, feature_type, feature_name):
        if feature_type == "h3":
            centroid = h3.h3_to_geo(pandas_data[feature_name][0])
            # set to centroid of a geom
            mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[0]
            mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[1]
        elif feature_type == "geometry":
            tmp_sdf = config.mosaic_spark.createDataFrame(pandas_data.iloc[:1])
            centroid = (
                tmp_sdf.select(st_centroid2D(col(feature_name)))
                .limit(1)
                .collect()[0][0]
            )
            # set to centroid of a geom
            mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[1]
            mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[0]

    @cell_magic
    def mosaic_kepler(self, *args):
        """Replace current line with new output"""
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
        elif feature_type == "geometry":
            data = data.withColumn(feature_name, st_astext(col(feature_name)))
        else:
            raise Exception(f"Unsupported geometry type: {feature_type}.")

        allowed_schema = [
            field.name
            for field in data.schema.fields
            if field.dataType.typeName() in ["string", "long", "integer", "double"]
        ]
        data = data.select(*allowed_schema)
        to_render = data.limit(limit_ctn).cache()
        pandas_data = to_render.limit(limit_ctn).toPandas()

        self.set_centroid(pandas_data, feature_type, feature_name)

        m1 = KeplerGl(config=mosaic_kepler_config)
        m1.add_data(data=pandas_data, name=table_name)

        logo_html = (
            f"<img src='data:image/png;base64, {mosaic_logo_b64str}' height='20px'>"
        )

        config.display_handler.display_html(logo_html)
        self.displayKepler(m1, 800, 1200)
