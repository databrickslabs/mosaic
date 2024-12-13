__all__ = [
    "df_to_interim"
]

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

def df_to_interim(
        df: DataFrame, tbl: str, overwrite: bool, skip_if_exists: bool = False,
        max_records_per_file: int = 100_000, do_max_records: bool = True,
        add_seed_col: bool = False, seed_col_name: str = "seed_group", num_seed: int = 10_000,
        debug: int = 1
) -> DataFrame:
  """
  Helper function to write dataframes to interim tables with partitioning (alt to `repartition`).
  - The final table is written to delta lake.
  - The write will use either overwrite (also allows overwriteSchema)  or append based on `overwrite`.
  - The write can be skipped based on `skip_if_exists`, default False.
  - Each file size is controlled by `max_records_per_file`,
    default 100_000 if do_max_records (default True)
  - Can add a column that places rows in a 'seed_group' column, default is False and 10K groups.
  - The operations sets the following table properties to false (avoid partition coalescing):
    (a) delta.autoOptimize.optimizeWrite
    (b) delta.autoOptimize.autoCompact
  Returns DataFrame backed by the written table.
  """
  _spark = SparkSession.builder.getOrCreate()

  options = {}
  if do_max_records:
    options['maxRecordsPerFile'] = max_records_per_file
  save_mode = "append"
  if overwrite:
    debug > 1 and print("... overwrite")
    if _spark.catalog.tableExists(tbl):
      _spark.sql(f"delete from {tbl}")
    options["overwriteSchema"] = "true"
    save_mode = "overwrite"

  if not skip_if_exists or not _spark.catalog.tableExists(tbl):

    _df = df
    if add_seed_col:
      _df = (
        df
          .withColumn(
            seed_col_name,
            F.ceil(F.rand(seed=42) * F.lit(num_seed))
        )
      )

    (
      _df
        .write
        .format("delta")
            .options(**options)
            .mode(save_mode)
        .saveAsTable(tbl)
    )
    if do_max_records:
      _spark.sql(
        f"ALTER TABLE {tbl} SET TBLPROPERTIES" +
            f"(" +
            f"delta.autoOptimize.optimizeWrite = false" +
            f", delta.autoOptimize.autoCompact = false" +
            f")"
      )
  df_tbl = _spark.table(tbl)
  debug > 0 and print(f"... count {tbl}? {df_tbl.count():,}")
  if debug > 1:
    df_tbl.show(1)
  return df_tbl