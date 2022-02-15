import json

import h3
from IPython.core.magic import Magics, cell_magic, magics_class
from keplergl import KeplerGl
from pyspark.sql import functions as F

from mosaic.config import config

try:
    from PythonShellImpl.PythonShell import displayHTML
except ImportError:

    def displayHTML(html: str):
        print(html)


with open("mosaic/utils/kepler_config.json", "r") as fh:
    mosaic_kepler_config = json.load(fh)


@magics_class
class MosaicKepler(Magics):
    def displayKepler(self, map_instance, height, width):
        displayHTML(
            map_instance._repr_html_()
            .decode("utf-8")
            .replace(".height||400", f".height||{height}")
        )

    @cell_magic
    def mosaic_kepler(self, *args):
        "Replace current line with new output"

        inputs = [
            i
            for i in " ".join(list(args)).replace("\n", " ").replace('"', "").split(" ")
            if len(i) > 0
        ]

        if len(inputs) != 3 and len(inputs) != 4:
            raise Exception(
                "Mosaic Kepler magic requires table name, feature column and feature type all to be provided. Limit is optional (default 1000)."
            )

        table_name = inputs[0]
        feature_name = inputs[1]
        feature_type = inputs[2]
        limitCtn = 1000
        if len(inputs) == 4:
            limitCtn = int(inputs[3])

        data = config.mosaic_spark.read.table(table_name)
        feature_col_dt = [dt for dt in data.dtypes if dt[0] == feature_name][0]

        if feature_type == "h3":
            if feature_col_dt[1] == "bigint":
                data = data.withColumn(
                    feature_name, F.lower(F.conv(F.col(feature_name), 10, 16))
                )
        elif feature_type == "geometry":
            raise Exception(f"Usupported geometry type: {feature_type}.")
        else:
            raise Exception(f"Usupported geometry type: {feature_type}.")

        toRender = data.limit(limitCtn).cache()
        pandasData = toRender.limit(limitCtn).toPandas()

        centroid = h3.h3_to_geo(pandasData[feature_name][0])
        mosaic_kepler_config["config"]["mapState"]["latitude"] = centroid[
            0
        ]  # set to centroid of a geom
        mosaic_kepler_config["config"]["mapState"]["longitude"] = centroid[
            1
        ]  # se to centrodi of a geom

        m1 = KeplerGl(config=mosaic_kepler_config)
        m1.add_data(data=pandasData, name=table_name)

        logo_html = "<img src='/files/milos_colic/mosaic_logo.png' height='20px'>"

        displayHTML(logo_html)
        self.displayKepler(m1, 800, 1200)
