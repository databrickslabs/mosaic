package com.dblabs.spatial.vectorx.ds.ogr

import com.dblabs.spatial.rasterx.gdal.GDALManager
import com.dblabs.spatial.util.{NodeFileManager, SerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

class OGR_Reader(partition: OGR_Partition) extends PartitionReader[InternalRow] {

    GDALManager.init(partition.expressionConfig)
    OGR_SchemaInference.enableOGRDrivers()
    NodeFileManager.init(partition.expressionConfig.hConf)

    private val tmpPath = NodeFileManager.readRemote(partition.filePath)
    private val dataset = OGR_SchemaInference.getDataSource(partition.driver, tmpPath)
    private val layer = dataset.GetLayer(partition.layer)
    layer.ResetReading()
    layer.SetNextByIndex(partition.start)
    private var counter = partition.start

    private var nextRow: InternalRow = _

    override def next(): Boolean = {
        nextRow = null
        val feature = layer.GetNextFeature()
        if (counter < partition.end && feature != null) {
            val row = OGR_SchemaInference.getFeatureFields(feature, partition.schema, partition.asWKB)
            nextRow = SerializationUtil.createRow(row)
            counter = counter + 1
            true
        } else {
            close()
            false
        }
    }

    override def get(): InternalRow = nextRow

    override def close(): Unit = NodeFileManager.releaseRemote(partition.filePath)

}
