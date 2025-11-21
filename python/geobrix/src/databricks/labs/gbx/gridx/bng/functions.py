from pyspark.sql import functions as f
from pyspark.sql import SparkSession


def register(_spark):
    # register functions via the reader
    _spark = SparkSession.builder.getOrCreate()
    _spark.read.format("register_ds").option("functions", "gridx.bng").load().collect()

def bng_aswkb(cell_id):
    return f.call_function("gbx_bng_aswkb", cell_id)

def bng_aswkt(cell_id):
    return f.call_function("gbx_bng_aswkt", cell_id)

def bng_cellarea(cell_id):
    return f.call_function("gbx_bng_cellarea", cell_id)

def bng_cellintersection(cell_id1, cell_id2):
    return f.call_function("gbx_bng_cellintersection", cell_id1, cell_id2)

def bng_cellunion(cell_id1, cell_id2):
    return f.call_function("gbx_bng_cellunion", cell_id1, cell_id2)

def bng_centroid(cell_id):
    return f.call_function("gbx_bng_centroid", cell_id)

def bng_distance(cell_id1, cell_id2):
    return f.call_function("gbx_bng_distance", cell_id1, cell_id2)

def bng_eastnortasbng(east, north, resolution):
    return f.call_function("gbx_bng_eastnortasbng", east, north, resolution)

def bng_euclideandistance(cell_id1, cell_id2):
    return f.call_function("gbx_bng_euclideandistance", cell_id1, cell_id2)

def bng_geometrykloop(geom, resolution, k):
    return f.call_function("gbx_bng_geometrykloop", geom, resolution, k)

def bng_geometrykring(geom, resolution, k):
    return f.call_function("gbx_bng_geometrykring", geom, resolution, k)

def bng_pointasbng(point, resolution):
    return f.call_function("gbx_bng_pointasbng", point, resolution)

def bng_polyfill(geom, resolution):
    return f.call_function("gbx_bng_polyfill", geom, resolution)

def bng_tessellate(geom, resolution):
    return f.call_function("gbx_bng_tessellate", geom, resolution)

# Aggregators

def bng_cellintersection_agg(cells):
    return f.call_function("gbx_bng_cellintersection_agg", cells)

def bng_cellunion_agg(cells):
    return f.call_function("gbx_bng_cellunion_agg", cells)


# Generators

def bng_geometrykloopexplode(geom, resolution, k):
    return f.explode(f.call_function("gbx_bng_geometrykloopexplode", geom, resolution, k))

def bng_geometrykringexplode(geom, resolution, k):
    return f.explode(f.call_function("gbx_bng_geometrykringexplode", geom, resolution, k))

def bng_kloopexplode(cell_id, k):
    return f.explode(f.call_function("gbx_bng_kloopexplode", cell_id, k))

def bng_kringexplode(cell_id, k):
    return f.explode(f.call_function("gbx_bng_kringexplode", cell_id, k))

def bng_tessellateexplode(geom, resolution):
    return f.explode(f.call_function("gbx_bng_tessellateexplode", geom, resolution))