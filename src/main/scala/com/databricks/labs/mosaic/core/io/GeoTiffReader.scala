package com.databricks.labs.mosaic.core.io

import java.awt.image.BufferedImage

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.commons.imaging.{FormatCompliance, ImageReadException}
import org.apache.commons.imaging.common.bytesource.ByteSourceArray
import org.apache.commons.imaging.formats.tiff.{TiffDirectory, TiffField, TiffReader}
import org.apache.commons.imaging.formats.tiff.constants.GeoTiffTagConstants
import org.apache.commons.imaging.formats.tiff.taginfos.TagInfoDoubles

class GeoTiffReader {

    def geoTiffTags(ifd: TiffDirectory): Option[mutable.Map[GeoKey.Value, Any]] = {
        val geoKeyDirectory = Try(ifd.getFieldValue(GeoTiffTagConstants.EXIF_TAG_GEO_KEY_DIRECTORY_TAG, true)) match {
            case Success(k)                     => k
            case Failure(_: ImageReadException) => return None
        }
        val elements = new Array[Int](geoKeyDirectory.length)
        for (i <- 0 until geoKeyDirectory.length) {
            elements(i) = geoKeyDirectory(i) & 0xffff
        }
        val doubleParameters = ifd.findField(GeoTiffTagConstants.EXIF_TAG_GEO_DOUBLE_PARAMS_TAG) match {
            case null         => None
            case f: TiffField => Some(f.getDoubleArrayValue)
        }
        val asciiParameters = ifd.findField(GeoTiffTagConstants.EXIF_TAG_GEO_ASCII_PARAMS_TAG) match {
            case null         => None
            case f: TiffField => Some(f.getStringValue)
        }
        val tags = mutable.Map[GeoKey.Value, Any]()

        var k = 0
        val n = elements.length / 4
        for (i <- 0 until n) {
            val key = elements(k)
            val ref = elements(k + 1)
            val len = elements(k + 2)
            val vop = elements(k + 3)
            if (key >= GeoKey.values.map(_.id).min) {
                val value = ref match {
                    case i: Int if i == GeoTiffTagConstants.EXIF_TAG_GEO_DOUBLE_PARAMS_TAG.tag => doubleParameters.get.slice(vop, vop + len)
                    case i: Int if i == GeoTiffTagConstants.EXIF_TAG_GEO_ASCII_PARAMS_TAG.tag  =>
                        asciiParameters.get.slice(vop, vop + len - 1).mkString("")
                    case _                                                                     => vop
                }
                tags += (GeoKey(key) -> value)
            }
            k += 4
        }
        Some(tags)
    }

    def fromBytes(bytes: Array[Byte]): BufferedImage = {
        val byteSource = new ByteSourceArray("", bytes)
        val reader = new TiffReader(true)
        val content = reader.readDirectories(byteSource, true, FormatCompliance.getDefault)
        val layers = content.directories.asScala
            .map(t => (geoTiffTags(t), t))
            .filter(_._1.isDefined)
            .map(f => (f._1.get, f._2.getTiffImage))
        layers.head._2
    }

    // The following elements were copied from the original
    // GeoTIFF specification document
    //     Ritter, Niles and Ruth, Mike (1995). GeoTIFF Format Specification,
    //     GeoTIFF Revision 1.0. Specification Version 1.8.1. 31 October 1995
    //     Appendix 6.
    // See also:
    //     Open Geospatial Consortium [OGC] (2019) OGC GeoTIFF Standard Version 1.1
    //     http://www.opengis.net/doc/IS/GeoTIFF/1.1
    object GeoKey extends Enumeration {

        type GeoKey = Value

        // From 6.2.1 GeoTiff Configuration Keys
        val GTModelTypeGeoKey: GeoKey.Value = Value(1024) /* Section 6.3.1.1 Codes       */
        val GTRasterTypeGeoKey: GeoKey.Value = Value(1025) /* Section 6.3.1.2 Codes       */
        val GTCitationGeoKey: GeoKey.Value = Value(1026) /* documentation */

        // From 6.2.2 Geographic Coordinate System Parameter Keys
        val GeographicTypeGeoKey: GeoKey.Value = Value(2048) /* Section 6.3.2.1 Codes     */
        val GeogCitationGeoKey: GeoKey.Value = Value(2049) /* documentation             */
        val GeogGeodeticDatumGeoKey: GeoKey.Value = Value(2050) /* Section 6.3.2.2 Codes     */
        val GeogPrimeMeridianGeoKey: GeoKey.Value = Value(2051) /* Section 6.3.2.4 codes     */
        val GeogLinearUnitsGeoKey: GeoKey.Value = Value(2052) /* Section 6.3.1.3 Codes     */
        val GeogLinearUnitSizeGeoKey: GeoKey.Value = Value(2053) /* meters                    */
        val GeogAngularUnitsGeoKey: GeoKey.Value = Value(2054) /* Section 6.3.1.4 Codes     */
        val GeogAngularUnitSizeGeoKey: GeoKey.Value = Value(2055) /* radians                   */
        val GeogEllipsoidGeoKey: GeoKey.Value = Value(2056) /* Section 6.3.2.3 Codes     */
        val GeogSemiMajorAxisGeoKey: GeoKey.Value = Value(2057) /* GeogLinearUnits           */
        val GeogSemiMinorAxisGeoKey: GeoKey.Value = Value(2058) /* GeogLinearUnits           */
        val GeogInvFlatteningGeoKey: GeoKey.Value = Value(2059) /* ratio                     */
        val GeogAzimuthUnitsGeoKey: GeoKey.Value = Value(2060) /* Section 6.3.1.4 Codes     */
        val GeogPrimeMeridianLongGeoKey: GeoKey.Value = Value(2061) /* GeogAngularUnit           */

        // From 6.2.3 Projected Coordinate System Parameter Keys
        val ProjectedCRSGeoKey: GeoKey.Value = Value(3072) /* Section 6.3.3.1 codes   */
        val PCSCitationGeoKey: GeoKey.Value = Value(3073) /* documentation           */
        val ProjectionGeoKey: GeoKey.Value = Value(3074) /* Section 6.3.3.2 codes   */
        val ProjCoordTransGeoKey: GeoKey.Value = Value(3075) /* Section 6.3.3.3 codes   */
        val ProjLinearUnitsGeoKey: GeoKey.Value = Value(3076) /* Section 6.3.1.3 codes   */
        val ProjLinearUnitSizeGeoKey: GeoKey.Value = Value(3077) /* meters                  */
        val ProjStdParallel1GeoKey: GeoKey.Value = Value(3078) /* GeogAngularUnit */
        val ProjStdParallel2GeoKey: GeoKey.Value = Value(3079) /* GeogAngularUnit */
        val ProjNatOriginLongGeoKey: GeoKey.Value = Value(3080) /* GeogAngularUnit */
        val ProjNatOriginLatGeoKey: GeoKey.Value = Value(3081) /* GeogAngularUnit */
        val ProjFalseEastingGeoKey: GeoKey.Value = Value(3082) /* ProjLinearUnits */
        val ProjFalseNorthingGeoKey: GeoKey.Value = Value(3083) /* ProjLinearUnits */
        val ProjFalseOriginLongGeoKey: GeoKey.Value = Value(3084) /* GeogAngularUnit */
        val ProjFalseOriginLatGeoKey: GeoKey.Value = Value(3085) /* GeogAngularUnit */
        val ProjFalseOriginEastingGeoKey: GeoKey.Value = Value(3086) /* ProjLinearUnits */
        val ProjFalseOriginNorthingGeoKey: GeoKey.Value = Value(3087) /* ProjLinearUnits */
        val ProjCenterLongGeoKey: GeoKey.Value = Value(3088) /* GeogAngularUnit */
        val ProjCenterLatGeoKey: GeoKey.Value = Value(3089) /* GeogAngularUnit */
        val ProjCenterEastingGeoKey: GeoKey.Value = Value(3090) /* ProjLinearUnits */
        val ProjCenterNorthingGeoKey: GeoKey.Value = Value(3091) /* ProjLinearUnits */
        val ProjScaleAtNatOriginGeoKey: GeoKey.Value = Value(3092) /* ratio   */
        val ProjScaleAtCenterGeoKey: GeoKey.Value = Value(3093) /* ratio   */
        val ProjAzimuthAngleGeoKey: GeoKey.Value = Value(3094) /* GeogAzimuthUnit */
        val ProjStraightVertPoleLongGeoKey: GeoKey.Value = Value(3095) /* GeogAngularUnit */

        // From 6.2.4 Vertical Coordinate System Keys
        val VerticalCSTypeGeoKey: GeoKey.Value = Value(4096) /* Section 6.3.4.1 codes   */
        val VerticalCitationGeoKey: GeoKey.Value = Value(4097) /* documentation */
        val VerticalDatumGeoKey: GeoKey.Value = Value(4098) /* Section 6.3.4.2 codes   */
        val VerticalUnitsGeoKey: GeoKey.Value = Value(4099) /* Section 6.3.1.3 codes   */

        // Widely used key not defined in original specification
        val To_WGS84_GeoKey: GeoKey.Value = Value(2062) /* Not in original spec */

    }

    /**
      * Provides specifications for Coordinate Transformation Codes as defined
      * in Appendix 6.3.3.3 "Coordinate Transformation Codes" of the original
      * GeoTiff specification (Ritter, 1995).
      */
    object CoordinateTransformationCode extends Enumeration {

        type CoordinateTransformationCode = Value
        val TransverseMercator: CoordinateTransformationCode.Value = Value(1)
        val TransvMercator_Modified_Alaska: CoordinateTransformationCode.Value = Value(2)
        val ObliqueMercator: CoordinateTransformationCode.Value = Value(3)
        val ObliqueMercator_Laborde: CoordinateTransformationCode.Value = Value(4)
        val ObliqueMercator_Rosenmund: CoordinateTransformationCode.Value = Value(5)
        val ObliqueMercator_Spherical: CoordinateTransformationCode.Value = Value(6)
        val Mercator: CoordinateTransformationCode.Value = Value(7)
        val LambertConfConic_2SP: CoordinateTransformationCode.Value = Value(8)
        val LambertConfConic_Helmert: CoordinateTransformationCode.Value = Value(9)
        val LambertAzimEqualArea: CoordinateTransformationCode.Value = Value(10)
        val AlbersEqualArea: CoordinateTransformationCode.Value = Value(11)
        val AzimuthalEquidistant: CoordinateTransformationCode.Value = Value(12)
        val EquidistantConic: CoordinateTransformationCode.Value = Value(13)
        val Stereographic: CoordinateTransformationCode.Value = Value(14)
        val PolarStereographic: CoordinateTransformationCode.Value = Value(15)
        val ObliqueStereographic: CoordinateTransformationCode.Value = Value(16)
        val Equirectangular: CoordinateTransformationCode.Value = Value(17)
        val CassiniSoldner: CoordinateTransformationCode.Value = Value(18)
        val Gnomonic: CoordinateTransformationCode.Value = Value(19)
        val MillerCylindrical: CoordinateTransformationCode.Value = Value(20)
        val Orthographic: CoordinateTransformationCode.Value = Value(21)
        val Polyconic: CoordinateTransformationCode.Value = Value(22)
        val Robinson: CoordinateTransformationCode.Value = Value(23)
        val Sinusoidal: CoordinateTransformationCode.Value = Value(24)
        val VanDerGrinten: CoordinateTransformationCode.Value = Value(25)
        val NewZealandMapGrid: CoordinateTransformationCode.Value = Value(26)
        val TransvMercator_SouthOriented: CoordinateTransformationCode.Value = Value(27)

    }

}
