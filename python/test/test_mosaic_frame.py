from test.utils import MosaicTestCase

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame

from mosaic import MosaicFrame, displayMosaic


class TestMosaicFrame(MosaicTestCase):
    point_df: DataFrame
    poly_df: DataFrame
    point_mdf: MosaicFrame
    poly_mdf: MosaicFrame

    def setUp(self) -> None:
        self.point_df = self.generate_input_point_collection()
        self.poly_df = self.generate_input_polygon_collection()
        self.point_mdf = MosaicFrame(self.point_df, "geometry")
        self.poly_mdf = MosaicFrame(self.poly_df, "geometry")

    def test_get_optimal_resolution(self):
        result = self.poly_mdf.get_optimal_resolution(sample_fraction=1.0)
        self.assertEqual(result, 9)

    def test_count_rows(self):
        self.assertEqual(self.point_mdf.count(), 100_000)
        self.assertEqual(self.poly_mdf.count(), 263)

    def test_join(self):
        joined_df = (
            self.point_mdf.set_index_resolution(9)
            .apply_index()
            .join(self.poly_mdf.set_index_resolution(9).apply_index())
        )
        self.assertEqual(joined_df.count(), 100_000)
        self.assertEqual(len(joined_df.columns), 19)

    def test_pretty_print(self):
        displayMosaic(self.point_mdf)
