from py4j.protocol import Py4JJavaError

from test.utils import MosaicTestCase
from .context import api


class TestMosaicAnalyzer(MosaicTestCase):
    def test_get_optimal_resolution(self):
        test_df = self.generate_input_polygon_collection()
        analyser = api.MosaicAnalyzer()
        analyser.set_sample_fraction(1.0)
        result = analyser.get_optimal_resolution(test_df, "geom")
        self.assertEqual(result, 9)

    def test_fails_if_undersampled(self):
        test_df = self.generate_input_polygon_collection()
        analyser = api.MosaicAnalyzer()
        with self.assertRaises(Py4JJavaError):
            analyser.get_optimal_resolution(test_df, "geom")
