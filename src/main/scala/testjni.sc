import org.gdal.gdal.gdal

val dataset = gdal.Open("resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF");
val band = dataset.GetRasterBand(1)

