from functools import reduce

from pyspark.sql import DataFrame

from mosaic import MosaicFrame
from mosaic.core.mosaic_frame import MosaicJoinType
from test.utils import MosaicTestCase


class TestMosaicFrame(MosaicTestCase):
    def test_join_types(self):
        self.assertEqual(MosaicJoinType().point_in_polygon.toString(), "PointInPolygon")
        self.assertEqual(MosaicJoinType().polygon_intersection.toString(), "PolygonIntersection")

    def test_get_resolution_metrics(self):
        def union_df(left: DataFrame, right: DataFrame) -> DataFrame:
            return left.unionAll(right)
        df = reduce(union_df, [self.generate_input_polygon_collection() for i in range(10)])
        print(df.count())
        mdf = MosaicFrame(df, "geometry")
        result = mdf.get_resolution_metrics().collect()
        self.assertEqual(result, 10)

    def test_join(self):
        self.fail()
