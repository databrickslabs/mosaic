from pyspark.sql import SparkSession
from typing import Any
import shutil
import subprocess
import tempfile

__all__ = ["setup_gdal", "enable_gdal"]

def setup_fuse_install(
    spark: SparkSession, to_fuse_dir: str, with_mosaic_pip: bool, with_gdal: bool, 
    with_ubuntugis: bool = False, script_name: str = 'mosaic-fuse-init.sh', 
    override_mosaic_version: str = None, skip_jar_copy: bool = False
) -> None:
    """
    [1] Copies Mosaic "fat" JAR (with dependencies) into `to_fuse_dir`
        - by default, version will match the current mosaic version executing the command,
          assuming it is a released version; if `override_mosaic_version` is a single value, 
          versus a range, that value will be used instead
        - this doesn't involve a script unless `with_mosaic_pip=True` or `with_gdal=True`
        - if `skip_jar_copy=True`, then the JAR is not copied
    [2] if `with_mosaic_pip=True`
        - configures script that configures to pip install databricks-mosaic==$MOSAIC_VERSION 
          or to `override_mosaic_version`
        - this is useful (1) to "pin" to a specific mosaic version, especially if using the
           JAR that is also being pre-staged for this version and (2) to consolidate all mosaic
           setup into a script and avoid needing to `%pip install databricks-mosaic` in each session
    [3] if `with_gdal=True`
        - configures script that is a variation of what setup_gdal does with some differences
        - configures to load shared objects from fuse dir (vs wget)
    [4] if `with_ubuntugis=True` (assumes `with_gdal=True`)   
        - configures script to use the GDAL version provided by ubuntugis
        - default is False
    Notes:
      (a) `to_fuse_dir` can be one of `/Volumes/..`, `/Workspace/..`, `/dbfs/..`
      (b) Volume paths are the recommended FUSE mount for Databricks in DBR 13.3+ 
      (c) If using Volumes, there are more admin actions that a Unity Catalog admin
          needs to be take to add the generated script and JAR to the Unity Catalog 
          allowlist, essential steps for Shared Cluster and Java access!
      (d) `FUSE_DIR` within the script will be set to the passed value 

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.
    to_fuse_dir : str
            Path to write out the resource(s) for GDAL installation.
    with_mosaic_pip : bool
            Whether to configure a script that pip installs databricks-mosaic, 
            fixed to the current version.
    with_gdal : bool
            Whether to also configure a script for GDAL and pre-stage GDAL JNI shared object files.
    with_ubuntugis : bool
            Whether to use ubuntugis ppa for GDAL instead of built-in;
            default is False.
    script_name : str
            name of the script to be written;
            default is 'mosaic-fuse-init.sh'.
    override_mosaic_version: str
            String value to use to override the mosaic version to install,
            e.g. '0.4.0' or '<0.5,>=0.4';
            default is None.
    skip_jar_copy: bool
            Whether to skip copying the Mosaic JAR;
            default is False.

    Returns
    -------
    """
    print("TODO")

     
def setup_gdal(
    spark: SparkSession, to_fuse_dir: str = '/dbfs/FileStore/geospatial/mosaic/gdal/jammy',
    with_mosaic_pip: bool, with_ubuntugis: bool = False, script_name: str = 'mosaic-gdal-init.sh',
    override_mosaic_version: str = None
) -> None:
    """
    Prepare GDAL init script and shared objects required for GDAL to run on spark.
    This function will generate the init script that will install GDAL on each worker node.
    After the setup_gdal is run, the init script must be added to the cluster; also,
    a cluster restart is required. 
    
    Notes:
      (a) This is close in behavior to Mosaic < 0.4 series (prior to DBR 13), with new options
          to pip install Mosaic for either ubuntugis gdal (3.4.3) or jammy default (3.4.1)
      (b) `to_fuse_dir` can be one of `/Volumes/..`, `/Workspace/..`, `/dbfs/..`;
           however, you should use `setup_fuse_install()` for Volume based installs
      (c) The init script generated will be named value of `script_name`, 
          default: 'mosaic-gdal-init.sh'
    
    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.
    to_fuse_dir : str
            Path to write out the init script for GDAL installation;
            default is '/dbfs/FileStore/geospatial/mosaic/gdal/jammy'.
    with_mosaic_pip : bool
            Whether to configure a script that pip installs databricks-mosaic, 
            fixed to the current version.
     with_ubuntugis : bool
            Whether to use ubuntugis ppa for GDAL instead of built-in;
            default is False.
    script_name : str
            name of the script to be written;
            default is 'mosaic-gdal-init.sh'.
    override_mosaic_version: str
            String value to use to override the mosaic version to install,
            e.g. '0.4.0' or '<0.5,>=0.4';
            default is None.

    Returns
    -------

    """
    # this is the output from the scala configuration
    out_init_script_filename = 'mosaic-gdal-init.sh'
    
    # - for volumes, java copy to local dir first
    to_dir = to_fuse_dir
    if to_fuse_dir.startswith('/Volumes/'):
        d = tempfile.TemporaryDirectory(dir = '/tmp')
        to_dir = d.name

    # - execute with java
    #   passing either local or fuse dir as `to_dir`
    #   passing True | False for also copying JNI so files
    mosaicGDALObject = getattr(
        spark.sparkContext._jvm.com.databricks.labs.mosaic.gdal, "MosaicGDAL"
    )
    mosaicGDALObject.prepareEnvironment(spark._jsparkSession, to_dir, jni_so_files)
    
    # - for volumes
    #   replace the FUSE_DIR path in the init script and 
    #   copy to the specified volume
    if to_fuse_dir.startswith('/Volumes/'):
        # [1a] read existing local init script and replace local path
        with open(f'{to_dir}/{out_init_script_filename}', 'r') as i_file:
            filedata = i_file.read().replace(to_dir, to_fuse_dir)

        # [1b] write the local init script out again 
        with open(f'{to_dir}/{out_init_script_filename}', 'w') as i_file:
            i_file.write(filedata)
        
        # [2] copy from local to fuse dir
        #     this will include shared objects, if specified
        shutil.copytree(to_dir, to_fuse_dir) 

    # - echo status
    print("GDAL setup complete.\n")
    print(f"Init script configured and stored as: '{to_fuse_dir}/{out_init_script_filename}'.\n")
    (jni_so_files == True) and print(f"... JNI Shared Objects also copied under '{to_fuse_dir}'.")
    print(
        "Please add the init script to your cluster and restart to complete the setup.\n"
    )


def enable_gdal(spark: SparkSession) -> None:
    """
    Enable GDAL at runtime on a cluster with GDAL installed using the init script generated by setup_gdal() call.

    Parameters
    ----------
    spark : pyspark.sql.SparkSession
            The active SparkSession.

    Returns
    -------

    """
    try:
        sc = spark.sparkContext
        mosaicGDALObject = getattr(
            sc._jvm.com.databricks.labs.mosaic.gdal, "MosaicGDAL"
        )
        mosaicGDALObject.enableGDAL(spark._jsparkSession)
        print("GDAL enabled.\n")
        result = subprocess.run(["gdalinfo", "--version"], stdout=subprocess.PIPE)
        print(result.stdout.decode() + "\n")
    except Exception as e:
        print(
            "GDAL not enabled. Mosaic with GDAL requires that GDAL be installed on the cluster.\n"
        )
        print(
            "Please run setup_gdal() to generate the init script for install GDAL install.\n"
        )
        print(
            "After the init script is generated, please add the init script to your cluster and restart to complete the setup.\n"
        )
        print("Error: " + str(e))
