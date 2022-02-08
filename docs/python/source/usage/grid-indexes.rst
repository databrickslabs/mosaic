==================================
Using grid index systems in Mosaic
==================================

Set operations over big geospatial datasets become very expensive without some form of spatial indexing.

Spatial indexes not only allow operations like point-in-polygon joins to be partitioned but, if only approximate results
are required, can be used to reduce these to deterministic SQL joins directly on the indexes.

The workflow for a point-in-poly join would then be:

- Choose an appropriate resolution for your grid index.
- Apply the index to the set of points in your left-hand dataframe.
  This will generate generate an index value that corresponds to the grid 'cell' that this point occupies.
- Compute the set of indices that fully covers each polygon in the right-hand dataframe
  (this is referred to as a _polyfill_ operation).
- 'Explode' the polygon index dataframe, such that each polygon index becomes a row in a new dataframe.
- Join the new left- and right-hand dataframes directly on the index.

Choosing an appropriate resolution for your task:

- A resolution that generates ~10x the number of indices as polygons is recommended.
- You may have significant skew in the areas of your polygons. To check whether this is the case, and help choose an
  appropriate index resolution you may wish to look at using Mosaic's analysis tools.

Mosaic provides support for Uber's H3 spatial indexing library as a core part of the API, but the framework is designed
to be extensible. If you are considering plugging your own favourite library in to perform