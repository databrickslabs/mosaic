from test.utils import MosaicTestCase
from .context import api


class TestMosaicAnalyzer(MosaicTestCase):
    def test_get_optimal_resolution(self):
        test_df = self.generate_input_polygon_collection()
        analyser = api.MosaicAnalyzer()
        analyser.set_sample_fraction(1.0)
        result = analyser.get_optimal_resolution(test_df, "geom")
        self.assertEqual(result, 9)
