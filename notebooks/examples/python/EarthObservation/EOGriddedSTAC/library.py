from io import BytesIO
from matplotlib import pyplot
from rasterio.io import MemoryFile
from rasterio.plot import show

from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import *

import json
import mosaic as mos
import pandas as pd
import planetary_computer
import pystac_client
import rasterio
import requests
import shapely.geometry

FILE_SIZE_THRESHOLD = 1024
FILENAME_TIMESTAMP_FORMAT = "%Y%m%d-%H%M%S"

ps_client =  pystac_client.Client.open(
  "https://planetarycomputer.microsoft.com/api/stac/v1",
  modifier=planetary_computer.sign_inplace
)


def generate_cells(extent, resolution, spark, mos):
  polygon = shapely.geometry.box(*extent, ccw=True)
  wkt_poly = str(polygon.wkt)
  cells = spark.createDataFrame([[wkt_poly]], ["geom"])
  cells = cells.withColumn("grid", mos.grid_tessellateexplode("geom", F.lit(resolution)))
  return cells


@udf(returnType=ArrayType(StringType()))
def get_assets(item):
  item_dict = json.loads(item)
  assets = item_dict["assets"]
  return [json.dumps({**{"name": asset}, **assets[asset]}) for asset in assets]


@pandas_udf(ArrayType(StringType()))
def get_items(geojsons: pd.Series, date_times: pd.Series, collections: pd.Series) -> pd.Series:

  from tenacity import retry, wait_exponential

  @retry(wait=wait_exponential(multiplier=2, min=4, max=240))
  def search_with_retry(geojson, catalog, collection, dt):
    search = catalog.search(
        collections = collection,
        intersects = geojson,
        datetime = dt
      )
    items = search.item_collection()
    return [json.dumps(item.to_dict()) for item in items]

  def search_catalog(geojson, catalog, collection, dt):
    try:
      return search_with_retry(geojson, catalog, collection, dt)
    except Exception as inst:
      return [str(inst)]

  catalog =  pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
  )
  # - iterate over all the series at once
  items = []
  for geojson, collection, date_time in zip(geojsons, collections, date_times):
    items.append(
      search_catalog(geojson, catalog, collection, date_time)
    )
  return pd.Series(items)


def get_assets_for_cells(cells_df, period, source, spark, repart_num=512):
  try:
    orig_repart_num = spark.conf.get("spark.sql.shuffle.partitions")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", False)
    spark.conf.set("spark.sql.shuffle.partitions", repart_num)
    print(f"\t...shuffle partitions to {repart_num} for this operation.")
    return (
      cells_df
      .repartition(repart_num)
        .withColumn("items", get_items("geojson", F.lit(period), F.array(F.lit(source))))
      .repartition(repart_num)
        .withColumn("items", F.explode("items"))
        .withColumn("assets", get_assets("items"))
      .repartition(repart_num)
        .withColumn("assets", F.explode("assets"))
        .withColumn("asset", F.from_json(F.col("assets"), MapType(StringType(), StringType())))
        .withColumn("item", F.from_json(F.col("items"), MapType(StringType(), StringType())))
        .withColumn("item_properties", F.from_json("item.properties", MapType(StringType(), StringType())))
        .withColumn("item_collection", F.col("item.collection"))
        .withColumn("timestamp", F.col("item_properties").getItem("datetime").cast("timestamp"))
        .withColumn("date", F.col("timestamp").cast("date"))
        .withColumn("item_bbox", F.col("item.bbox"))
        .withColumn("item_id", F.col("item.id"))
        .withColumn("stac_version", F.col("item.stac_version"))
      .drop("assets", "items", "item")
      .repartition(repart_num, "item_id")
    )
  finally:
    # print(f"...setting shuffle partitions back to {orig_repart_num}")
    spark.conf.set("spark.sql.shuffle.partitions", orig_repart_num)


def get_unique_hrefs(assets_df, item_name):
  return (
    assets_df
      .select(
        "area_id",
        "h3",
        "asset.name",
        "asset.href",
        "item_id",
        F.to_date("item_properties.datetime").alias("date")
      )
      .where(
        f"name == '{item_name}'"
      )
      .groupBy(
      "href", "item_id", "date"
      )
      .agg(F.first("h3").alias("h3"))
  )


@pandas_udf(StringType())
def download_asset(
  item_ids:pd.Series, asset_names:pd.Series, dir_fuse_paths:pd.Series, out_filenames:pd.Series
) -> pd.Series:
  """
  Do not accept an asset as downloaded below the size threshold.
  - this is because Planetary Computer will provide a message instead of
    the actual data when free tier limits are being hit or
    urls not signed (possibly expired)
  - write outpaths below size_threshold to dir_fuse_invalids
  - asset href is signed here to ensure it does not go stale
  """
  from tenacity import retry, wait_exponential
  import os
  import pandas as pd
  import pystac_client
  import planetary_computer
  import requests

  @retry(wait=wait_exponential(multiplier=2, min=4, max=240))
  def download_href(href, outpath):
    # Make the actual request, set the timeout for no data to 10 seconds and enable streaming responses so we don't have to keep the large files in memory
    request = requests.get(href, timeout=100, stream=True)

    # Open the output file and make sure we write in binary mode
    with open(outpath, 'wb') as fh:
      # Walk through the request response in chunks of 1024 * 1024 bytes, so 1MiB
      for chunk in request.iter_content(1024 * 1024):
        # Write the chunk to the file
        fh.write(chunk)
        # Optionally we can check here if the download is taking too long
    return outpath

  def write_asset(catalog, item_id, asset_name, out_dir, filename):
    """
    """
    size_threshold = 1024
    try:
      # - make sure out dir exists
      os.makedirs(out_dir, exist_ok=True)

      # - outpath assembled
      outpath = f'{out_dir}/{filename}'
      if not os.path.exists(outpath) or os.path.getsize(outpath) <= size_threshold:
        # - get the asset by asset_id and asset_name href
        item = next(catalog.get_items(item_id), None)
        return download_href(item.assets[asset_name].href, outpath)
      else:
        #print(f"...skipping '{outpath}', already exits. Size? {os.path.getsize(outpath)}")
        return outpath
    except Exception as error:
      #print("EXCEPTION: ", error)
      return None

  # - construct catalog
  catalog =  pystac_client.Client.open(
    "https://planetarycomputer.microsoft.com/api/stac/v1",
    modifier=planetary_computer.sign_inplace
  )

  # - iterate over all the series at once
  out_file_paths = []
  for item_id, asset_name, dir_fuse_path, out_filename in zip(item_ids, asset_names, dir_fuse_paths, out_filenames):
    out_file_paths.append(
      write_asset(catalog, item_id, asset_name, dir_fuse_path, out_filename)
    )
  return pd.Series(out_file_paths)


def to_numpy_arr(raster):
  with MemoryFile(BytesIO(raster)) as memfile:
    with memfile.open() as src:
      return src.read()


def plot_raster(raster, fig_w=8, fig_h=8):
  fig, ax = pyplot.subplots(1, figsize=(fig_w, fig_h))

  with MemoryFile(BytesIO(raster)) as memfile:
    with memfile.open() as src:
      show(src, ax=ax)
      pyplot.show()


def plot_file(file_path, fig_w=8, fig_h=8):
  fig, ax = pyplot.subplots(1,  figsize=(fig_w, fig_h))

  with rasterio.open(file_path) as src:
    show(src, ax=ax)
    pyplot.show()


def rasterio_lambda(raster, lambda_f):
  @udf(returnType=DoubleType())
  def f_udf(f_raster):
    with MemoryFile(BytesIO(f_raster)) as memfile:
      with memfile.open() as dataset:
        x = lambda_f(dataset)
        return float(x)

  return f_udf(raster)
