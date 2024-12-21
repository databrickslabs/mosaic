__all__ = [
    "RioUtils",
]
from matplotlib.figure import Figure
from typing import Union

import numpy as np
import pandas as pd
import rasterio

class RioUtils:

    @staticmethod
    def figsize_scale(ds:rasterio.DatasetReader, scale_factor:float=1.0) -> Figure:
        """Construct matplotlib Figure scaled to rasterio Dataset.

        - uses specified threshold_mb
        - if file already below threshold, it will be returned as a list

        Parameters
        ----------
        ds : rasterio.DatasetReader
            Dataset to scale
        scale_factor: float (optional)
            scale percentage, e.g. 1.0 is 100%;
            default 1.0
        Returns
        -------
        matplotlib Figure
        """
        from matplotlib import pyplot as plt

        h_px, w_px = (ds.shape[0], ds.shape[1]) # <- ds (h,w)
        print(f"h_px, w_px -> {h_px}, {w_px}")

        fig_scale = (scale_factor/100) * np.array([w_px, h_px])
        print(f"fig_scale -> {fig_scale}")

        return plt.figure(figsize=fig_scale) # <- figsize (w,h)

    @staticmethod
    def try_reproject_tif(path_in:str, path_out:str, dst_crs:str='EPSG:4326') -> Union[bool,None]:
        """Try to reproject a tif using its path.

        Parameters
        ----------
        path_in : str
            Path to read the raster file to be reprojected
        path_out: str
            Path to write the reprojected rasters
        dst_crs: str (optional)
            The CRS to reproject path_in;
            default is 'EPSG:4326'
        Returns
        -------
        bool | None
            Whether operation was successful; None if the path_out already exists
        """
        import os
        import rasterio
        import shutil
        import tempfile
        from pathlib import Path
        from rasterio.warp import calculate_default_transform, reproject, Resampling

        try:
            if os.path.exists(path_out):
                print(f"... skipping existing path_out -> '{path_out}'")
                return None

            # [1] open the input tif
            with rasterio.open(path_in) as src:
                transform, width, height = calculate_default_transform(
                    src.crs, dst_crs, src.width, src.height, *src.bounds)
                kwargs = src.meta.copy()
                kwargs.update({
                    'crs': dst_crs,
                    'transform': transform,
                    'width': width,
                    'height': height
                })

                # [2] reproject to dst_crs
                out_fuse_dir = os.path.dirname(path_out)
                Path(out_fuse_dir).mkdir(parents=True, exist_ok=True)
                if src.crs != dst_crs:
                    # TODO: STANDARDIZE THIS
                    # - (a) use a local temp file
                    with tempfile.TemporaryDirectory() as tmp_dir:
                        out_filename = os.path.basename(path_out)
                        tmp_path = f"{tmp_dir}/{out_filename}"
                        with rasterio.open(tmp_path, 'w', **kwargs) as dst:
                            for i in range(1, src.count + 1):
                                reproject(
                                    source=rasterio.band(src, i),
                                    destination=rasterio.band(dst, i),
                                    src_transform=src.transform,
                                    src_crs=src.crs,
                                    dst_transform=transform,
                                    dst_crs=dst_crs,
                                    resampling=Resampling.nearest
                                )

                        # - (b) copy local to path_out
                        shutil.copyfile(tmp_path, path_out)
                else:
                    # - (c) skip reproject
                    #   copy path_in to path_out
                    print(f"... skipping transform (same crs) -> '{dst_crs}'")
                    shutil.copyfile(path_in, path_out)
                return True
        except Exception as e:
            print(f"ERROR `try_reproject_tif`: {e}")
            return False

    @staticmethod
    def try_dataset_to_h3_pdf(
            ds:rasterio.DatasetReader, h3_res:int, band_idx:int=1, nodata_value=np.NAN,
            compact:bool=False, axis_order:str='yx', val_type=None
    ) -> Union[pd.DataFrame, None]:
        """Try to transform a rasterio Dataset to a h3 resolution, and return a pandas DataFrame.

        Parameters
        ----------
        ds : rasterio.DatasetReader
            Dataset for the operation
        h3_res: int
            h3 resolution to use in the transform
        band_idx: int (optional)
            raster band to use in the transform;
            default is 1
        nodata_value: type (optional)
            value for nodata;
            default is np.NAN
        compact: bool (optional)
            Whether to compact the h3 resolutions;
            default is False
        axis_order: str (optional)
            Order of x and y;
            default is 'yx'
        val_type: type
            Specify the output value type used in the np array;
            default is None
        Returns
        -------
        pandas DataFrame

        See Also:
            h3ronpy.polars.raster.raster_to_dataframe - Notes on defaults:
                - compact - False (instead of True in ref)
                - val_type e.g. np.float32 (instead of None in ref)
        """
        from h3ronpy.polars.raster import raster_to_dataframe
        import os
        import polars as pl

        try:
            band = ds.read(band_idx)
            if val_type is not None:
                band = band.astype(val_type)
            podf = raster_to_dataframe(
                band,
                ds.transform,
                h3_res,
                axis_order=axis_order,
                nodata_value=nodata_value,
                compact=compact,
            )

            # add the filename to the output
            path = ds.name
            filename = os.path.basename(path)
            podf = podf.with_columns(
                pl.lit(filename)
                .alias("filename")
            )
            podf = podf.rename({'cell':'cellid'})
            return podf.to_pandas()
        except Exception as e:
            print(f"ERROR `try_dataset_h3_dataframe`: {e}")
            return None

    @staticmethod
    def try_del_dataset_files(ds_path:str, driver:str=None) -> bool:
        """Try to delete a rasterio dataset using its path.

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
        from rasterio.shutil import delete as rio_delete

        try:
            rio_delete(ds_path, driver=driver)
            return True
        except Exception as e:
            print(f"ERROR `try_del_dataset` ({ds_path}): {e}")
            return False

    @staticmethod
    def try_copy_dataset_files(ds_path:str, to_path:str) -> bool:
        """Try to copy the dataset files.

        1. This function copies all associated files with the dataset,
           including auxiliary files like world files (.wld) or projection files (.prj)
        2. It does not open the dataset itself,
           so it can be faster than using rasterio.copy
        3. If the destination directory does not exist, it will be created

        Parameters
        ----------
        ds_path : str
            Most likely a fuse path to read the raster file(s) to be copied
        to_path : str
            Most likely a fuse path to copy the raster file(s)
        Returns
        -------
        bool
            True if success; otherwise False
        """
        from rasterio.shutil import copyfiles as rio_copyfiles

        try:
            rio_copyfiles(ds_path, to_path)
            return True
        except Exception as e:
            print(f"ERROR `try_copy_dataset_files` ({ds_path}, {to_path}): {e}")
            return False

    @staticmethod
    def try_subdivide_tif(in_path: str, out_dir: str, threshold_mb: int = 8) -> list[str]:
        """try to subdivide a geotiff raster.

        1. Import necessary libraries including rasterio
        2. Get the filesize: use the stored filesize as basis of subdivide
        3. Read the raster: Open the raster file using rasterio.open
        4. Subdivide the raster: Use the metadata to subdivide the raster

        Parameters
        ----------
        in_path : str
            Most likely a fuse path to read the raster being subdivided
        out_dir : str
            Most likely a fuse dir to write the subdivided rasters
        threshold_mb: int, options
            The threshold in megabytes for each subdivided raster
        Returns
        -------
        list[str]
            A list of the paths to the subdivided rasters; if the filesize is less
            than the threshold, return list with path to the raster;
            if an issue occurs, return empty list
        """
        from rasterio.windows import Window
        from rasterio.shutil import exists as rio_exists
        import math
        import os
        import rasterio
        import tempfile

        try:
            results = []
            os.makedirs(out_dir, exist_ok=True)

            # get in_filename
            # don't need the extension
            in_fn = os.path.splitext(os.path.basename(in_path))[0]
            out_ext = "tif"

            # get filesize
            # - return in_fuse_path if < threshold
            raster_mb = math.ceil(os.path.getsize(in_path) / (1024 * 1024))
            if raster_mb <= threshold_mb:
                return [in_path]

            # determine the subdivide number
            # - find the next nearest square root for cols and rows
            subdivide_num = math.ceil(math.sqrt(raster_mb / threshold_mb))

            # subdivide the raster
            with rasterio.Env():
                # Read the raster
                out_image, out_transform, out_meta = (None, None, None)
                with rasterio.open(in_path) as src:
                    # update metadata for the clipped raster
                    split_meta = src.meta.copy()
                    split_meta.update(driver='GTiff')

                    # split height and weight
                    src_height, src_width = (src.shape[0], src.shape[1])
                    w, w_mod, w_sum = (math.floor(src_width / subdivide_num), src_width % subdivide_num, 0)
                    h, h_mod, h_sum = (math.floor(src_height / subdivide_num), src_height % subdivide_num, 0)
                    print(
                        f"::: src_width: {src_width} src_height: {src_height} subdivide_num: {subdivide_num} (mod_w:{w_mod} mod_h: {h_mod}) :::")

                    for i in range(subdivide_num):
                        # split width pixels (cols)
                        # - add an extra pixel as needed
                        split_w = w if i >= w_mod else w + 1
                        split_meta.update(width=split_w)
                        h_sum = 0  # <- reset per loop

                        for j in range(subdivide_num):
                            # split height pixels (rows)
                            # - add an exra pixel as needed
                            split_h = h if j >= h_mod else h + 1

                            print(f"...{i}-{j} -> splitting w:{split_w} | h:{split_h} (o_w: {w_sum}, o_h: {h_sum})")

                            # window read
                            # - col offset, row offset, width, height
                            # - returns a numpy ndarray or a view of
                            split_win = Window(w_sum, h_sum, split_w, split_h)
                            split_data = src.read(window=split_win, boundless=True)  # <- allow beyond extent
                            split_transform = src.window_transform(split_win)
                            split_meta.update(transform=split_transform)

                            # write the subdivided raster
                            # - write to local then copy to out_path
                            split_meta.update(height=split_h)
                            out_fn = f"{in_fn}-{i}_{j}.{out_ext}"
                            out_path = f"{out_dir}/{out_fn}"
                            with tempfile.TemporaryDirectory() as tmp_dir:
                                tmp_path = f'{tmp_dir}/{out_fn}'
                                with rasterio.open(tmp_path, 'w', **split_meta) as dst:
                                    dst.write(split_data)
                                try:
                                    if rio_exists(out_path):
                                        RioUtils.try_del_dataset_files(out_path)
                                    if RioUtils.try_copy_dataset_files(tmp_path, out_path):
                                        results.append(out_path)
                                    else:
                                        msg = f"ERROR `try_subdivide_tif`: Failed to write tmp raster to {out_path}"
                                        print(msg)
                                        return []
                                finally:
                                    RioUtils.try_del_dataset_files(tmp_path)

                            # maintain height sum (inner loop)
                            h_sum += split_h

                        # maintain width sum (outer loop)
                        w_sum += split_w
            return results
        except Exception as e:
            msg = f"ERROR `try_subdivide_tif`: {e}"
            print(msg)
            return []