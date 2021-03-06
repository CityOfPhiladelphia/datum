import pyproj
from shapely.wkt import loads as shp_loads, dumps as shp_dumps
from shapely.ops import transform as shp_transform

class WktTransformer(object):
    def __init__(self, from_srid, to_srid):
        # TODO if a projected coordinate system is involved that does not use
        # feet, you need to pass in `preserve_units=True` into pyproj.Proj
        # so that it doesn't assume we're working with meters.
        self.project = partial(
            pyproj.transform,
            pyproj.Proj('+init=EPSG:{}'.format(from_srid)),
            pyproj.Proj('+init=EPSG:{}'.format(to_srid))
        )

    def transform(self, from_wkt):
        from_shp = shp_loads(from_wkt)
        shp_t = shp_transform(self.project, from_shp)
        return shp_dumps(shp_t)
