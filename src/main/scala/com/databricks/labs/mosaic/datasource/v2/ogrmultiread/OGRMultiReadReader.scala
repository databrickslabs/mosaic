package com.databricks.labs.mosaic.datasource.v2.ogrmultiread

import com.databricks.labs.mosaic.core.raster.api.GDAL
import com.databricks.labs.mosaic.datasource.OGRFileFormat
import com.databricks.labs.mosaic.utils.{HadoopUtils, ReaderUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class OGRMultiReadReader(partition: OGRMultiReadPartition) extends PartitionReader[InternalRow] {

    try {
        System.load("/usr/lib/gdalalljni.so")
    } catch {
        case _: Throwable =>
    }
    GDAL.enable()
    OGRFileFormat.enableOGRDrivers()

    private val hconf = partition.hConf
    private val tmpPath = HadoopUtils.copyToLocalTmp(partition.filePath, hconf)
    private val dataset = OGRFileFormat.getDataSource(partition.driver, tmpPath)
    private val layer = dataset.GetLayer(partition.layer)
    layer.ResetReading()
    layer.SetNextByIndex(partition.start)
    private var counter = partition.start

    private var nextRow: InternalRow = _

    override def next(): Boolean = {
        nextRow = null
        val feature = layer.GetNextFeature()
        if (counter < partition.end && feature != null) {
            val row = OGRFileFormat.getFeatureFields(feature, partition.schema, partition.asWKB)
            nextRow = ReaderUtils.createRow(row)
            counter = counter + 1
            true
        } else {
            close()
            false
        }
    }

    override def get(): InternalRow = nextRow

    override def close(): Unit = HadoopUtils.deleteIfExists(tmpPath, hconf)

}
