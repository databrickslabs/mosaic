from pyspark.sql import SparkSession
from typing import Any
import os
import pkg_resources
import requests
import subprocess

__all__ = ["setup_gdal", "enable_gdal"]

# TODO: CHANGE URL AFTER PR ACCEPTED
GITHUB_SCRIPT_URL = 'https://raw.githubusercontent.com/mjohns-databricks/mosaic/gdal-jammy-1/scripts/mosaic-gdal-init.sh'
SCRIPT_FUSE_DIR_TOKEN= 'FUSE_DIR=__FUSE_DIR__'
SCRIPT_MOSAIC_VERSION_TOKEN = 'MOSAIC_VERSION=__MOSAIC_VERSION__'
SCRIPT_MOSAIC_PIP_VERSION_TOKEN = 'MOSAIC_PIP_VERSION=__MOSAIC_PIP_VERSION__'
SCRIPT_WITH_MOSAIC_TOKEN = 'WITH_MOSAIC=0'
SCRIPT_WITH_UBUNTUGIS_TOKEN ='WITH_UBUNTUGIS=0' 
SCRIPT_WITH_FUSE_SO_TOKEN = 'WITH_FUSE_SO=0'

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
            e.g. '==0.4.0' or '<0.5,>=0.4';
            default is None.
    skip_jar_copy: bool
            Whether to skip copying the Mosaic JAR;
            default is False.

    Returns
    -------
    """
    print("TODO")

     
def setup_gdal(
    to_fuse_dir: str = '/dbfs/FileStore/geospatial/mosaic/gdal/jammy',
    with_mosaic_pip: bool = False, with_ubuntugis: bool = False, script_name: str = 'mosaic-gdal-init.sh',
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
    to_fuse_dir : str
            Path to write out the init script for GDAL installation;
            default is '/dbfs/FileStore/geospatial/mosaic/gdal/jammy'.
    with_mosaic_pip : bool
            Whether to configure a script that pip installs databricks-mosaic, 
            fixed to the current version; default is False.
     with_ubuntugis : bool
            Whether to use ubuntugis ppa for GDAL instead of built-in;
            default is False.
    script_name : str
            name of the script to be written;
            default is 'mosaic-gdal-init.sh'.
    override_mosaic_version: str
            String value to use to override the mosaic version to install,
            e.g. '==0.4.0' or '<0.5,>=0.4';
            default is None.

    Returns
    -------

    """
    # - current mosaic version
    mosaic_version = None
    try:
        mosaic_version = pkg_resources.get_distribution("databricks-mosaic").version
    except Exception:
        print(f"... could not parse current mosaic version, won't specify version")
        pass
    
    # - generate fuse dir path
    os.makedirs(to_fuse_dir, exist_ok=True)
    
    # - start with the unconfigured script
    script = requests.get(GITHUB_SCRIPT_URL, allow_redirects=True).content

    # - set the fuse dir
    script = script.replace(
        SCRIPT_FUSE_DIR_TOKEN, SCRIPT_FUSE_DIR_TOKEN.replace('__FUSE_DIR__', to_fuse_dir)
    )
    # - set the mosaic version for pip
    if override_mosaic_version is not None:
        script = script.replace(
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN, 
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN.replace(
                '__MOSAIC_PIP_VERSION__', override_mosaic_version)
            )
    elif mosaic_version is not None:
        script = script.replace(
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN, 
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN.replace(
                '__MOSAIC_PIP_VERSION__', mosaic_version)
            )
    else:
       script = script.replace(
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN, 
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN.replace(
                '__MOSAIC_PIP_VERSION__', '')
            )

    # - are we configuring for mosaic pip?
    if with_mosaic_pip:
        script = script.replace(
            SCRIPT_WITH_MOSAIC_TOKEN, SCRIPT_WITH_MOSAIC_TOKEN.replace('0','1')
        )    
        
    # - are we configuring for ubuntugis?
    if with_ubuntugis:
        script = script.replace(
            SCRIPT_WITH_UBUNTUGIS_TOKEN, SCRIPT_WITH_UBUNTUGIS_TOKEN.replace('0','1')
        )
     
    # - write the configured init script
    with open(f'{to_fuse_dir}/{script_name}', 'w') as file:
        file.write(script)
        
    # - echo status
    print("GDAL setup complete.\n")
    print(f"Init script configured and stored as: '{to_fuse_dir}/{script_name}'.\n")
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
