## Proposed approach for storing all geometry types in our InternalGeometry representation

geom example|typeName|`boundary` example|`boundary` dimensions|`holes` example|`holes` dimensions
---|---|---|---|---|---
Point|`Point`| `Array(Array(Array(1.0, 0.0)))`|1, 1, {2..4}|`Array(Array(Array()))`|0, 0, 0
MultiPoint|`MultiPoint`|`Array(Array(Array(1.0, 0.0)), Array(Array(2.0, 1.0)))`|M, 1, {2..4}|`Array(Array(Array()))`|0, 0, 0
LineString|`LineString`|`Array(Array(Array(1.0 0.0), Array(2.0, 1.0)))`|1, M, {2..4}|`Array(Array(Array()))`|0, 0, 0
MultiLineString|`MultiLineString`|`Array(Array(Array(1.0 0.0), Array(2.0, 1.0)), Array(Array(2.0, 2.0), Array(2.0, 1.0)))`|M, M, {2..4}|`Array(Array(Array()))`|0, 0, 0
Polygon|`Polygon`|`Array(Array(Array(30, 10), Array(40, 40), Array(20, 40), Array(10, 20), Array(30, 10)))`|1, M, {2..4}|`Array(Array(Array()))`|0, 0, 0
Polygon (with hole)|`Polygon`|`Array(Array(Array(35, 10), Array(45, 45), Array(15, 40), Array(10, 20), Array(35, 10)))`|1, M, {2..4}|`Array(Array(Array(20, 30), Array(35, 35), Array(20, 20), Array(20, 30)))`|1, M, {2..4}
MultiPolygon|`MultiPolygon`|`Array(Array(Array(30, 20), Array(45, 40), Array(10, 40), Array(30, 20)), Array(Array(15, 5), Array(40, 10), Array(10, 20), Array(5, 10), Array(15, 5)))`|M, M, {2..4}|`Array(Array(Array()))`|0, 0, 0
MultiPolygon (with hole)|`MultiPolygon`|`Array(Array(Array(35, 10), Array(45, 45), Array(15, 40), Array(10, 20), Array(35, 10)), Array(Array(15, 5), Array(40, 10), Array(10, 20), Array(5, 10), Array(15, 5)))`|M, M, {2..4}|`Array(Array(Array(20, 30), Array(35, 35), Array(20, 20), Array(20, 30)))`|1, M, {2..4}