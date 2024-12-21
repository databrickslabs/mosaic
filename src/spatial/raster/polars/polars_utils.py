__all__ = [
    "PolarsUtils",
]

import geopandas as gpd
import polars

class PolarsUtils:

    @staticmethod
    def h3_df_to_gdf(df_polars:polars.DataFrame, h3_col:str='cellid') -> gpd.GeoDataFrame:
        """Convert a polars dataframe with an h3 column to a geopandas dataframe.

        Parameters
        ----------
        ds_path : str
            Most likely a fuse path to read the raster file(s) to be copied
        driver : str (optional)
            The driver to use to open the dataset (default None);
            if not provided, use the driver registered to the extension
        Returns
        -------
        bool
            True if success; otherwise False
        """
        from ...grid.h3.h3_utils import H3Utils

        return H3Utils.h3_pdf_to_gdf(df_polars.to_pandas(), h3_col)