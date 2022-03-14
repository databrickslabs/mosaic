from test.utils import MosaicTestCase

from mosaic import displayMosaic, st_geomfromwkt, st_makepolygon


class TestDisplayHandler(MosaicTestCase):
    def test_display(self):
        df = self.wkt_boroughs()
        poly_df = df.select(st_makepolygon(st_geomfromwkt("wkt")).alias("polygon_geom"))
        displayMosaic(poly_df)
