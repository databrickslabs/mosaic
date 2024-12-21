__all__ = [
    "nearest_h3_res_udf",
    "reproject_tif_4326_udf",
    "subdivide_tif_udf",
    "subdivide_tif_mb_udf",
]

from pyspark.sql.functions import udf
from pyspark.sql.types import *

@udf(returnType=IntegerType())
def nearest_h3_res_udf(fuse_path:str):
    """Find the nearest h3 resolution to the provided raster path.

    Parameters
    ----------
    fuse_path : str
        Path to read the raster file to be copied;
        converts provided path into fuse format
    Returns
    -------
    int
        h3 resolution matching "smaller_than_pixel" or
        max h3 resolution (15) if pixel spatial resolution
        is on cm scale (vs m, km scale)

    See Also:
        h3ronpy.polars.raster.nearest_h3_resolution
    """
    from h3ronpy.polars.raster import nearest_h3_resolution
    from .. import Utils
    import rasterio

    path = Utils.path_as_fuse(fuse_path)
    ds = rasterio.open(path)
    return nearest_h3_resolution(ds.shape, ds.transform, search_mode="smaller_than_pixel")

@udf(returnType=BooleanType())
def reproject_tif_4326_udf(fuse_path_in: str, fuse_path_out: str):
    """Reproject tif and write to specified path.

    Parameters
    ----------
    fuse_path_in : str
        Path to read the raster file to be reprojected;
        converts provided path into fuse format
    fuse_path_out: str
        Path to write the reprojected raster;
        converts provided path into fuse format,
        skips reproject if scr and dst already the same CRS
    Returns
    -------
    bool
        Whether the operation was successful;
        None if the path already existed

    See Also:
        RioUtils.try_reproject_tif
    """
    from .rasterio.rasterio_utils import RioUtils
    from .. import Utils

    path_in = Utils.path_as_fuse(fuse_path_in)
    path_out = Utils.path_as_fuse(fuse_path_out)

    return RioUtils.try_reproject_tif(path_in, path_out,  dst_crs='EPSG:4326')

@udf(returnType=ArrayType(StringType()))
def subdivide_tif_udf(fuse_path_in: str, fuse_dir_out: str):
    """Subdivide tif and write to specified directory.

    - defaults to threshold_mb=8
    - if file already below threshold, it will be returned as a list

    Parameters
    ----------
    fuse_path_in : str
        Path to read the raster file to be subdivided;
        converts provided path into fuse format
    fuse_dir_out: str
        Directory to write the subdivided rasters;
        converts provided path into fuse format
    Returns
    -------
    list[str]
        Returns list of subdivided filepaths;
        will be empty if an error occurs

    See Also:
        RioUtils.try_subdivide_tif
    """
    from .rasterio.rasterio_utils import RioUtils
    from .. import Utils

    path_in = Utils.path_as_fuse(fuse_path_in)
    dir_out = Utils.path_as_fuse(fuse_dir_out)
    return RioUtils.try_subdivide_tif(path_in, dir_out, threshold_mb=8)

@udf(returnType=ArrayType(StringType()))
def subdivide_tif_mb_udf(fuse_path_in: str, fuse_dir_out: str, threshold_mb: int):
    """Subdivide tif and write to specified directory.

    - uses specified threshold_mb
    - if file already below threshold, it will be returned as a list

    Parameters
    ----------
    fuse_path_in : str
        Path to read the raster file to be subdivided;
        converts provided path into fuse format
    fuse_dir_out: str
        Directory to write the subdivided rasters;
        converts provided path into fuse format
    threshold_mb : int
        Size in MBs to split the original file
    Returns
    -------
    list[str]
        Returns list of subdivided filepaths;
        will be empty if an error occurs

    See Also:
        RioUtils.try_subdivide_tif
    """
    from .rasterio.rasterio_utils import RioUtils
    from .. import Utils

    path_in = Utils.path_as_fuse(fuse_path_in)
    dir_out = Utils.path_as_fuse(fuse_dir_out)
    return RioUtils.try_subdivide_tif(path_in, dir_out, threshold_mb=threshold_mb)