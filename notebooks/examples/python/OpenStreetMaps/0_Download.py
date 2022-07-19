# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Download Open Street Maps data
# MAGIC 
# MAGIC This notebook will download the latest OSM data from the [official source](https://download.geofabrik.de/) and convert it to Delta tables.
# MAGIC 
# MAGIC This example uses the [OSM XML](https://wiki.openstreetmap.org/wiki/OSM_XML) data format as input, but you can also use other tools like [osm-parquetizer](https://github.com/adrianulbona/osm-parquetizer) or [spark-osm-datasource](https://github.com/woltapp/spark-osm-datasource) to read the protobuf (pbf) format.
# MAGIC 
# MAGIC The [OSM is structured](https://wiki.openstreetmap.org/wiki/Elements) in three main element types:
# MAGIC 
# MAGIC * nodes (defining points in space)
# MAGIC * ways (defining linear features and area boundaries)
# MAGIC * relations (defining complex multipolygon relations or used to explain how other elements work together)
# MAGIC 
# MAGIC This notebook will create one delta table for each of this element types.
# MAGIC 
# MAGIC ![Diagram](https://raw.githubusercontent.com/databrickslabs/mosaic/main/notebooks/examples/python/OpenStreetMaps/Images/0_Download.png)
# MAGIC 
# MAGIC ## Note: INSTALL XML libraries before running this notebook!
# MAGIC 
# MAGIC From your cluster settings, install from Maven the xml reader library `com.databricks:spark-xml_2.12:0.14.0`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Settings
# MAGIC 
# MAGIC Define where to store the raw data and which region to download

# COMMAND ----------

# Define where the data will be stored
raw_path = f"dbfs:/tmp/mosaic/open_street_maps/"
mirror_index_url = "https://download.geofabrik.de/index-v1-nogeom.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC 
# MAGIC Creating the empty folder if it does not exist already

# COMMAND ----------

import pathlib

# The DBFS file system is mounted under /dbfs/ directory on Databricks cluster nodes
local_osm_path = pathlib.Path(raw_path.replace("dbfs:/", "/dbfs/"))

(local_osm_path / "raw").mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore available regions 

# COMMAND ----------

import requests

resp = requests.get(mirror_index_url)
features = resp.json()["features"]
datasets = list(map(lambda x: x["properties"], features))

available_regions = list(map(lambda x: f"{x['id']} ({ x.get('parent') })", datasets))
available_regions

# COMMAND ----------

# Define which regions to download
regions = [
  'italy',
#   'africa', 
#   'antarctica', 
#   'asia', 
#   'australia-oceania', 
#   'central-america', 
#   'europe',
#   'north-america',
#   'south-america'
] # See available regions few cell above

print(f"The raw Open Street Maps data for {regions} will be stored in {raw_path}")

# COMMAND ----------

# Uncomment the following line to clear all raw data
# dbutils.fs.rm(raw_path + "raw", True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Download
# MAGIC 
# MAGIC Download the region files concurrently

# COMMAND ----------

from multiprocessing import Pool

def download_region(region):
  selected_dataset = list(filter(lambda x: x["id"] == region, datasets))[0]
  compressed_osm_file = local_osm_path / "raw" / f"{region}.osm.bz2"
  osm_url = selected_dataset["urls"]["bz2"]

  # Download the file incrementally
  with requests.get(osm_url, stream=True) as r:
    r.raise_for_status()
    with open(compressed_osm_file, 'wb') as f:
      for chunk in r.iter_content(chunk_size=512*1024):
        f.write(chunk)

with Pool(4) as pool:
  pool.map(download_region, regions)

# COMMAND ----------

display(dbutils.fs.ls(raw_path + "raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define schema
# MAGIC 
# MAGIC We are explicitly defining the schemas, because auto-inference
# MAGIC would read an extra time the XML files.

# COMMAND ----------

from pyspark.sql.types import LongType, TimestampType, BooleanType, IntegerType, StringType, StructField, ArrayType, StructType, DoubleType

id_field = StructField("_id", LongType(), True)

common_fields = [
    StructField("_timestamp", TimestampType(), True),
    StructField("_visible", BooleanType(), True),
    StructField("_version", IntegerType(), True),
    StructField(
        "tag",
        ArrayType(
            StructType(
                [
                    StructField("_VALUE", StringType(), True),
                    StructField("_k", StringType(), True),
                    StructField("_v", StringType(), True),
                ]
            )
        ),
        True,
    ),
]

nodes_schema = StructType(
    [
        id_field,
        StructField("_lat", DoubleType(), True),
        StructField("_lon", DoubleType(), True),
        *common_fields,
    ]
)

ways_schema = StructType(
    [
        id_field,
        *common_fields,
        StructField(
            "nd",
            ArrayType(
                StructType(
                    [
                        StructField("_VALUE", StringType(), True),
                        StructField("_ref", LongType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

relations_schema = StructType(
    [
        id_field,
        *common_fields,
        StructField(
            "member",
            ArrayType(
                StructType(
                    [
                        StructField("_VALUE", StringType(), True),
                        StructField("_ref", LongType(), True),
                        StructField("_role", StringType(), True),
                        StructField("_type", StringType(), True),
                    ]
                )
            ),
            True,
        ),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest bronze layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Nodes

# COMMAND ----------

nodes = (spark
      .read
      .format("xml")
      .options(rowTag="node") # Only extract nodes
      .load(f"{raw_path}/raw/", schema = nodes_schema)
     )

nodes.write.format("delta").mode("overwrite").save(f"{raw_path}/bronze/nodes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ways

# COMMAND ----------

ways = (spark
      .read
      .format("xml")
      .options(rowTag="way") # Only extract ways
      .load(f"{raw_path}/raw/", schema = ways_schema)
     )

ways.write.format("delta").mode("overwrite").save(f"{raw_path}/bronze/ways")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Relations

# COMMAND ----------

relations = (spark
      .read
      .format("xml")
      .options(rowTag="relation") # Only extract relations
      .load(f"{raw_path}/raw/", schema=relations_schema)
     )

relations.write.format("delta").mode("overwrite").save(f"{raw_path}/bronze/relations")

# COMMAND ----------


