# README: Init Scripts

> Init scripts, libraries, and JVM based accesses have been changing in Databricks, more [here](https://docs.databricks.com/en/init-scripts/index.html#what-are-init-scripts). _We recommend Volume based Init Scripts as that is the future (and current) best practice for DBR 13.3+._

__Init Scripts Provided__

<p/> 

_All of these init scripts are for Ubuntu 22.04 (Jammy) which is available starting with DBR 13 series._

* __'install-gdal-fuse.sh'__ - options are '/Volumes/..', '/Workspace/..', and '/dbfs/..' FUSE mounts
  * You can configure and pre-stage this script with Mosaic `setup_gdal()` for python or `prepareEnvironment()` for scala; it becomes 'mosaic-gdal-init.sh'
  * Shown further down is a manual example of using bash to manually pre-stage init script and 'so' files into a Volume
  * '/Volumes' are recommended; they are required for Unity Catalog with Shared Clusters
* __'install-gdal-databricks.sh'__ - this is a non-Volume install script that installs the (Jammy) default GDAL 3.4.1
* __'install-gdal-ubuntugis.sh'__ - this is a non-Volume install script that installs the [Ubuntugis PPA](https://launchpad.net/~ubuntugis/+archive/ubuntu/ppa) (Jammy) GDAL 3.4.3

_Once your have your init script in an FUSE path pre-staged (and potentially the '.so' files depending on your environment requirements), then you need to add the script to your DBR cluster, see [here](https://docs.databricks.com/en/init-scripts/cluster-scoped.html#use-cluster-scoped-init-scripts) for instructions._ 

__Notes for Shared Access Clusters + Unity Catalog:__

<p/>

_You would focus on 'install-gdal-fuse.sh' (using Volume path), with slight modification for your environment as discussed below._

1. Volume based init scripts are required and must be added to the allowlist
3. Only Volume based files are allowed in init scripts which includes the GDAL Shared Objects(.so) files  
4. Not shown here but any external JARs (including Mosaic) must be added to the allowlist
    *  Shared Access clusters are required for JVM Access to Volume paths; otherwise just have SQL and Python access to Volumes
    *  Though Mosaic offers python bindings, it is written in Scava (JVM) which means it has these requirements
5. More on allowlists - [Allowlist libraries and init scripts on shared compute](https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/allowlist.html#add-an-init-script-to-the-allowlist) | [Volumes](https://docs.databricks.com/en/data-governance/unity-catalog/create-volumes.html) | [Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

_Here is an example set of manual bash calls to pre-stage init script 'install-gdal-fuse.sh' + shared object files in a '/Volume' location; you would run this on the driver._ As mentioned above, you can configure and pre-stage the 'install-gdal-fuse.sh' script with Mosaic `setup_gdal()` for python or `prepareEnvironment()` for scala 

```
%sh
# !!! you will need to setup a Volume within Unity Catalog before running this (in a notebook cell) !!!
$VOLUME_TARGET=<some_volume_path>

# - [a] download init script locally

$LOCAL_TARGET=/databricks/driver/transfer
mkdir -p $LOCAL_TARGET
wget -P $LOCAL_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/scripts/install-gdal-fuse.sh

# - [b] set the '$FUSE_DIR' within the generic script
#     you probably want to verify all went well
#     '-i' means 'in place'
sed -i 's,'FUSE_DIR=__FUSE_DIR__','FUSE_DIR="$VOLUME_TARGET"',' $LOCAL_TARGET/install-gdal-fuse.sh
# cat $LOCAL_TARGET/install-gdal-fuse.sh

# - [c] copy modified script to your Volume
cp $LOCAL_DIR/install-gdal-fuse.sh $VOLUME_TARGET

# - [d] download shared objects to your Volume
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30
wget -P $VOLUME_TARGET -nc https://github.com/databrickslabs/mosaic/raw/main/src/main/resources/gdal/ubuntu/libgdalalljni.so.30.0.3

ls $VOLUME_TARGET
```
