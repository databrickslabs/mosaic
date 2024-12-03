from pyspark.sql import SparkSession, DataFrame

__all__ = [
    "df_to_interim"
]

def df_to_interim(
        df: DataFrame, tbl: str, overwrite: bool,
        skip_if_exists: bool = False, max_records_per_file: int = 100_000, debug : int = 1
) -> DataFrame:
  """
  Helper function to write dataframes to tables.
  - The final table is written to delta lake.
  """
  _spark = SparkSession.builder.getOrCreate()

  options = {"maxRecordsPerFile": max_records_per_file}
  save_mode = "append"
  if overwrite:
    debug > 1 and print("... overwrite")
    _spark.sql(f"drop table if exists {tbl}")
    options["overwriteSchema"] = "true"
    save_mode = "overwrite"

  if not skip_if_exists or not _spark.catalog.tableExists(tbl):
    (
      df
        .write
        .format("delta")
            .options(**options)
            .mode(save_mode)
        .saveAsTable(tbl)
    )
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