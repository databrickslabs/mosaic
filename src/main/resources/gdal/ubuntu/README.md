# README: GDAL Shared Objects

> Mosaic requires GDAL JNI '.so' files be present for Java APIs. The 'install-gdal-*.sh' init scripts (provided within 'resources/scripts') can download the prebuilt shared object within this folder in two ways, described below. 

__[1] Directly download JNI shared objects.__ 

_The direct download is suitable when you are not using Unity Catalog + Shared Clusters._

```
# -- add pre-build JNI shared object to the path
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
wget -P /usr/lib -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30.0.3
```

__[2] Pre-stage JNI shared objects.__

_You must prestage in '/Volume' location to use Unity Catalog + Shared Clusters, more [here](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html)._

```
# -- add pre-build JNI shared object to the path
# !!! These should come from /Volumes !!!
cp $VOLUME_DIR/libgdalalljni.so /usr/lib
cp $VOLUME_DIR/libgdalalljni.so.30 /usr/lib
cp $VOLUME_DIR/libgdalalljni.so.30.0.3 /usr/lib
```
