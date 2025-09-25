from pyspark.sql import functions as f
from pyspark.sql import SparkSession


def register(_spark):
    # register functions via the reader
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "rasterx").load().collect()

def rst_avg(tile):
    return f.call_function("gbx_rst_avg", tile)

def rst_bandmetadata(tile, band):
    return f.call_function("gbx_rst_bandmetadata", tile, band)

def rst_boundingbox(tile):
    return f.call_function("gbx_rst_boundingbox", tile)

def rst_format(tile):
    return f.call_function("gbx_rst_format", tile)

def rst_georeference(tile):
    return f.call_function("gbx_rst_georeference", tile)

def rst_getnodata(tile):
    return f.call_function("gbx_rst_getnodata", tile)

def rst_getsubdataset(tile, subdataset):
    return f.call_function("gbx_rst_getsubdataset", tile, subdataset)

def rst_height(tile):
    return f.call_function("gbx_rst_height", tile)

def rst_max(tile):
    return f.call_function("gbx_rst_max", tile)

def rst_median(tile):
    return f.call_function("gbx_rst_median", tile)

def rst_memsize(tile):
    return f.call_function("gbx_rst_memsize", tile)

def rst_metadata(tile):
    return f.call_function("gbx_rst_metadata", tile)

def rst_min(tile):
    return f.call_function("gbx_rst_min", tile)

def rst_numbands(tile):
    return f.call_function("gbx_rst_numbands", tile)

def rst_pixelcount(tile):
    return f.call_function("gbx_rst_pixelcount", tile)

def rst_pixelheight(tile):
    return f.call_function("gbx_rst_pixelheight", tile)

def rst_pixelwidth(tile):
    return f.call_function("gbx_rst_pixelwidth", tile)

def rst_rotation(tile):
    return f.call_function("gbx_rst_rotation", tile)

def rst_scalex(tile):
    return f.call_function("gbx_rst_scalex", tile)

def rst_scaley(tile):
    return f.call_function("gbx_rst_scaley", tile)

def rst_skewx(tile):
    return f.call_function("gbx_rst_skewx", tile)

def rst_skewy(tile):
    return f.call_function("gbx_rst_skewy", tile)

def rst_srid(tile):
    return f.call_function("gbx_rst_srid", tile)

def rst_subdatasets(tile):
    return f.call_function("gbx_rst_subdatasets", tile)

def rst_summary(tile):
    return f.call_function("gbx_rst_summary", tile)

def rst_type(tile):
    return f.call_function("gbx_rst_type", tile)

def rst_upperleftx(tile):
    return f.call_function("gbx_rst_upperleftx", tile)

def rst_upperlefty(tile):
    return f.call_function("gbx_rst_upperlefty", tile)

def rst_width(tile):
    return f.call_function("gbx_rst_width", tile)

# Aggregators
def rst_combineavgagg(tiles):
    return f.call_function("gbx_rst_combineavgagg", tiles)

def rst_derivedbandagg(tiles, pyfunc, func_name):
    return f.call_function("gbx_rst_derivedbandagg", tiles, f.lit(pyfunc), f.lit(func_name))

def rst_mergeagg(tiles):
    return f.call_function("gbx_rst_mergeagg", tiles)

# Constructors
def rst_fromcontent(content, driver):
    return f.call_function("gbx_rst_fromcontent", content, driver)

def rst_fromfile(path, driver):
    return f.call_function("gbx_rst_fromfile", path, driver)

def from_bands(bands):
    return f.call_function("gbx_rst_frombands", bands)

# Generators
def rst_h3_tessellate(tile, resolution):
    return f.call_function("gbx_rst_h3_tessellate", tile, resolution)

def rst_maketiles(tile, tile_width, tile_height):
    return f.call_function("gbx_rst_maketiles", tile, tile_width, tile_height)

def rst_retile(tile, tile_width, tile_height):
    return f.call_function("gbx_rst_retile", tile, tile_width, tile_height)

def rst_separatebands(tile):
    return f.call_function("gbx_rst_separatebands", tile)

def rst_tooverlappingtiles(tile, tile_width, tile_height, overlap):
    return f.call_function("gbx_rst_tooverlappingtiles", tile, tile_width, tile_height, overlap)

# Grid
def rst_h3_rastertogridavg(tile, resolution):
    return f.call_function("gbx_rst_h3_rastertogridavg", tile, resolution)

def rst_h3_rastertogridcount(tile, resolution):
    return f.call_function("gbx_rst_h3_rastertogridcount", tile, resolution)

def rst_h3_rastertogridmax(tile, resolution):
    return f.call_function("gbx_rst_h3_rastertogridmax", tile, resolution)

def rst_h3_rastertogridmin(tile, resolution):
    return f.call_function("gbx_rst_h3_rastertogridmin", tile, resolution)

def rst_h3_rastertogridmedian(tile, resolution):
    return f.call_function("gbx_rst_h3_rastertogridmedian", tile, resolution)

# Operations
def rst_asformat(tile, driver):
    return f.call_function("gbx_rst_asformat", tile, driver)

def rst_clip(tile, clip, cutline_all_touched):
    return f.call_function("gbx_rst_clip", tile, clip, cutline_all_touched)

def rst_combineavg(tiles):
    return f.call_function("gbx_rst_combineavg", tiles)

def rst_convolve(tile, kernel):
    return f.call_function("gbx_rst_convolve", tile, kernel)

def rst_derivedband(tile, pyfunc, func_name):
    return f.call_function("gbx_rst_derivedband", tile, f.lit(pyfunc), f.lit(func_name))

def rst_dtmfromgeoms(geoms, pixel_size, extent):
    return f.call_function("gbx_rst_dtmfromgeoms", geoms, pixel_size, extent)

def rst_filter(tile, kernel_size, operation):
    return f.call_function("gbx_rst_filter", tile, kernel_size, operation)

def rst_initnodata(tile, nodata):
    return f.call_function("gbx_rst_initnodata", tile, nodata)

def rst_isempty(tile):
    return f.call_function("gbx_rst_isempty", tile)

def rst_mapalgebra(tiles, expression):
    return f.call_function("gbx_rst_mapalgebra", tiles, expression)

def rst_merge(tiles):
    return f.call_function("gbx_rst_merge", tiles)

def rst_ndvi(tile, nir_band, red_band):
    return f.call_function("gbx_rst_ndvi", tile, nir_band, red_band)

def rst_rastertoworldcoord(tile, x, y):
    return f.call_function("gbx_rst_rastertoworldcoord", tile, x, y)

def rst_rastertoworldcoordx(tile, x, y):
    return f.call_function("gbx_rst_rastertoworldcoordx", tile, x, y)

def rst_rastertoworldcoordy(tile, x, y):
    return f.call_function("gbx_rst_rastertoworldcoordy", tile, x, y)

def rst_transform(tile, target_srid):
    return f.call_function("gbx_rst_transform", tile, target_srid)

def rst_tryopen(path):
    return f.call_function("gbx_rst_tryopen", path)

def rst_updatetype(tile, new_type):
    return f.call_function("gbx_rst_updatetype", tile, new_type)

def rst_worldcoordtoraster(tile, x, y):
    return f.call_function("gbx_rst_worldcoordtoraster", tile, x, y)

def rst_worldcoordtorasterx(tile, x, y):
    return f.call_function("gbx_rst_worldcoordtorasterx", tile, x, y)

def rst_worldcoordtorastery(tile, x, y):
    return f.call_function("gbx_rst_worldcoordtorastery", tile, x, y)