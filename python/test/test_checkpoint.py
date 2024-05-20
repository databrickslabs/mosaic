from .context import api
from .utils import MosaicTestCaseWithGDAL
import os

class TestCheckpoint(MosaicTestCaseWithGDAL):
    def setUp(self) -> None:
        return super().setUp()

    def test_mode(self):
        self.assertEqual(
            self.spark.conf.get("spark.databricks.labs.mosaic.test.mode"), "true",
            "spark should have TEST_MODE set.")

    def test_context(self):
        self.assertIsNotNone(self.get_context(), "python context should exist.")
        self.assertTrue(self.get_context().has_context(), "jvm context should be initialized.")

    def test_checkpoint_path(self):
        self.assertEqual(
            self.get_context().get_checkpoint_path(), self.check_dir,
            "checkpoint path should equal dir.")
        self.assertEqual(
            self.get_context().get_checkpoint_path(),
            self.spark.conf.get("spark.databricks.labs.mosaic.raster.checkpoint"),
            "checkpoint path should equal spark conf.")

    def test_checkpoint_on(self):
        api.gdal.set_checkpoint_on(self.spark) # <- important to call from api.gdal
        self.assertTrue(self.get_context().is_use_checkpoint(), "context should be configured on.")

        # - test an operation
        result = (
            self.generate_singleband_raster_df()
            .withColumn("rst_boundingbox", api.rst_boundingbox("tile"))
            .withColumn("tile", api.rst_clip("tile", "rst_boundingbox"))
        )
        result.write.format("noop").mode("overwrite").save()
        self.assertEqual(result.count(), 1)
        tile = result.select("tile").first()[0]
        raster = tile['raster']
        self.assertIsInstance(raster, str, "raster type should be string.")

    def test_update_checkpoint(self):
        api.gdal.update_checkpoint_path(self.spark, self.new_check_dir) # <- important to call from api.gdal
        self.assertEqual(
            self.get_context().get_checkpoint_path(), self.new_check_dir,
            "context should be configured on.")
        self.assertTrue(os.path.exists(self.new_check_dir), "new check dir should exist.")

        # - test an operation
        result = (
            self.generate_singleband_raster_df()
            .withColumn("rst_boundingbox", api.rst_boundingbox("tile"))
            .withColumn("tile", api.rst_clip("tile", "rst_boundingbox"))
        )
        result.write.format("noop").mode("overwrite").save()
        self.assertEqual(result.count(), 1)
        tile = result.select("tile").first()[0]
        raster = tile['raster']
        self.assertIsInstance(raster, str, "raster type should be string.")

    def test_checkpoint_off(self):
        api.gdal.set_checkpoint_off(self.spark) # <- important to call from api.gdal
        self.assertFalse(self.get_context().is_use_checkpoint(), "context should be configured off.")

        # - test an operation
        result = (
            self.generate_singleband_raster_df()
            .withColumn("rst_boundingbox", api.rst_boundingbox("tile"))
            .withColumn("tile", api.rst_clip("tile", "rst_boundingbox"))
        )
        result.write.format("noop").mode("overwrite").save()
        self.assertEqual(result.count(), 1)
        tile = result.select("tile").first()[0]
        raster = tile['raster']
        self.assertNotIsInstance(raster, str, "raster type should be binary (not string).")
