package com.databricks.labs.mosaic.core.raster.io

import com.databricks.labs.mosaic.core.raster.gdal.MosaicRasterGDAL
import com.databricks.labs.mosaic.core.types.model.MosaicRasterTile

trait RasterHydrator {

    /**
     * Allows for recreation from file system or from content bytes.
     * - hydrate the underlying GDAL dataset, required call after destroy.
     * - recommend to always use this call when obtaining a raster for use in operation.
     * @param forceHydrate
     *   if true, rehydrate even if the dataset object exists; default is false.
     * @return
     *   Returns a hydrated (ready) [[MosaicRasterGDAL]] object.
     */
    def withHydratedDataset(forceHydrate: Boolean = false): MosaicRasterGDAL

    /**
     * Refreshes the raster dataset object. This is needed after writing to a file
     * system path. GDAL only properly writes to a file system path if the
     * raster object is destroyed. After refresh operation the raster object is
     * usable again.
     * - if already existing, flushes the cache of the raster and destroys. This is needed to ensure that the
     *   raster is written to disk. This is needed for operations like RasterProject.
     *
     * @return
     *   Returns [[MosaicRasterGDAL]].
     */
    def withDatasetRefreshFromPath(): MosaicRasterGDAL

}

/** singleton */
object RasterHydrator {

    /**
     * Hydrate the tile's raster.
     *
     * @param tile
     *   The [[MosaicRasterTile]] with the raster to hydrate.
     * @param forceHydrate
     *   if true, rehydrate even if the dataset object exists; default is false.
     */
    def withHydratedDataset(tile: MosaicRasterTile, forceHydrate: Boolean = false): MosaicRasterGDAL = {
        tile.raster.withHydratedDataset(forceHydrate = forceHydrate)
    }

}
