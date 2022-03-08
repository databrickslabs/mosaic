from pyspark.sql import DataFrame

from mosaic import MosaicFrame
from test.utils import MosaicTestCase


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

    def test_count_rows(self):
        self.assertEqual(self.point_mdf.count(), 100_000)
        self.assertEqual(self.poly_mdf.count(), 263)

    def test_analyzer(self):
        self.assertEqual(self.poly_mdf.get_optimal_resolution(200), 9)

    def test_join(self):
        joined_df = (
            self.point_mdf
                .set_index_resolution(9)
                .apply_index()
                .join(
                self.poly_mdf
                    .set_index_resolution(9)
                    .apply_index()
            )
        )
        self.assertEqual(joined_df.count(), 100_000)
        self.assertEqual(len(joined_df.columns), 19)
