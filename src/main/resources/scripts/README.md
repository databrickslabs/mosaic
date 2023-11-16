# README: Init Scripts

> Init scripts, libraries, and JVM based accesses have been changing in Databricks, more [here](https://docs.databricks.com/en/init-scripts/index.html#what-are-init-scripts). _We recommend Volume based Init Scripts as that is the future (and current) best practice for DBR 13.3+._

__Init Scripts Provided__

<p/> 

_All of these init scripts are for Ubuntu 22.04 (Jammy) which is available starting with DBR 13 series._

* 'install-gdal-volume.sh' - this recommended (and required) for Unity Catalog with Shared Clusters
* 'install-gdal-databricks.sh' - this is a non-Volume install script that installs the (Jammy) default GDAL 3.4.1
* 'install-gdal-ubuntugis.sh' - this is a non-Volume install script that installs the [Ubuntugis PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa) (Jammy) GDAL 3.4.3

__Notes for Shared Access Clusters + Unity Catalog:__

<p/>

_You would focus on 'install-gdal-volume.sh', with slight modification for your environment as discussed below._

1. Volume based init scripts are required and must be added to the allowlist
3. Only Volume based files are allowed in init scripts which includes the GDAL Shared Objects(.so) files  
4. Not shown here but any external JARs (including Mosaic) must be added to the allowlist
    *  Shared Access clusters are required for JVM Access to Volume paths; otherwise just have SQL and Python access to Volumes
    *  Though Mosaic offers python bindings, it is written in Scava (JVM) which means it has these requirements
5. More on allowlists - [Allowlist libraries and init scripts on shared compute](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html#add-an-init-script-to-the-allowlist) | [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html) | [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

_Here is an example set of calls to pre-stage an init script + shared object files in a '/Volume' location; you would run this on the driver._ 

```
%sh
# !!! you will need to setup a Volume within Unity Catalog before running this (in a notebook cell) !!!
$VOLUME_TARGET=<some_volume_path>

# - [a] download init script locally

$LOCAL_TARGET=/databricks/driver/transfer
mkdir -p $LOCAL_TARGET
wget -P $LOCAL_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/scripts/install-gdal-volume.sh

# - [b] set the '$VOLUME_DIR' within the generic script
#     you probably want to verify all went well
#     '-i' means 'in place'
sed -i 's,'VOLUME_DIR=/Volumes/geospatial_docs/gdal_artifacts/data','VOLUME_DIR="$VOLUME_TARGET"',' $LOCAL_TARGET/install-gdal-volume.sh
# cat $LOCAL_TARGET/install-gdal-volume.sh

# - [c] copy modified script to your Volume
cp $LOCAL_DIR/install-gdal-volume.sh $VOLUME_TARGET

# - [d] download shared objects to your Volume
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30.0.3

ls $VOLUME_TARGET
```
