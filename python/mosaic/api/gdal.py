from dataclasses import dataclass
from pyspark.sql import SparkSession
import os
import pkg_resources
import requests
import shutil
import subprocess

__all__ = ["setup_gdal", "enable_gdal"]

def get_install_mosaic_version() -> str:
    """
    Currently installed version of mosaic.

    Returns
    -------
    Installed version of package 'databricks-mosaic' if available;
    otherwise, None
    """
    try:
        return pkg_resources.get_distribution("databricks-mosaic").version
    except Exception:
        pass
    return None

@dataclass
class SetupMgr:
    """
    Defaults mirror setup_gdal.
    """
    to_fuse_dir: str
    script_in_name: str = 'mosaic-gdal-init.sh'
    script_out_name: str = 'mosaic-gdal-init.sh'
    with_mosaic_pip: bool = False
    with_gdal: bool = True
    with_ubuntugis: bool = False 
    override_mosaic_version: str = None
    jar_copy: bool = False
    jni_so_copy: bool = False

    def configure(self) -> None:
        """
        Handle various config options.
        - if `with_mosaic_pip` or `with_gdal` or `with_ubuntugis`,
          script will be configured and written.
        """
        # - set the mosaic and github versions
        #   will be used in downloading resources
        #   may be used in pip install 
        mosaic_version = get_install_mosaic_version()
        github_version = mosaic_version # <- valid or None
        if (
                self.override_mosaic_version is not None and 
                set(self.override_mosaic_version).issubset(set('=0123456789.'))
        ):
            github_version = self.override_mosaic_version.replace('=','') 
        elif mosaic_version is None:
            github_version = 'main'

        # TODOS AFTER PR MERGED: 
        # [1] CHANGE URL TO ACTUAL MOSAIC (not 'mjohns-databricks'):
        #     'https://raw.githubusercontent.com/databrickslabs/mosaic'
        # [2] USE f'{GITHUB_CONTENT_URL_BASE}/{github_version}' (not 'gdal-jammy-1')
        GITHUB_CONTENT_URL_BASE = 'https://raw.githubusercontent.com/mjohns-databricks/mosaic'
        GITHUB_CONTENT_TAG_URL = f'{GITHUB_CONTENT_URL_BASE}/gdal-jammy-1'

        # - generate fuse dir path
        os.makedirs(self.to_fuse_dir, exist_ok=True)

        with_script = self.with_mosaic_pip or self.with_gdal
        script_out_path = f'{self.to_fuse_dir}/{self.script_out_name}'
        if with_script:
            # - start with the unconfigured script
            script_url = f'{GITHUB_CONTENT_TAG_URL}/scripts/{self.script_in_name}'
            script = requests.get(script_url, allow_redirects=True).text
            
            # - tokens used in script
            SCRIPT_FUSE_DIR_TOKEN= "FUSE_DIR='__FUSE_DIR__'"                               # <- ' added
            SCRIPT_GITHUB_VERSION_TOKEN = 'GITHUB_VERSION=__GITHUB_VERSION__'
            SCRIPT_MOSAIC_PIP_VERSION_TOKEN = "MOSAIC_PIP_VERSION='__MOSAIC_PIP_VERSION__'" # <- ' added
            SCRIPT_WITH_MOSAIC_TOKEN = 'WITH_MOSAIC=0'
            SCRIPT_WITH_GDAL_TOKEN = 'WITH_GDAL=0'
            SCRIPT_WITH_UBUNTUGIS_TOKEN ='WITH_UBUNTUGIS=0' 
            SCRIPT_WITH_FUSE_SO_TOKEN = 'WITH_FUSE_SO=0'

            # - set the github version in the script
            #   this will be used to download so files
            script = script.replace(
                SCRIPT_GITHUB_VERSION_TOKEN, SCRIPT_GITHUB_VERSION_TOKEN.replace(
                    '__GITHUB_VERSION__', github_version)
            ) 

            # - set the fuse dir
            script = script.replace(
                SCRIPT_FUSE_DIR_TOKEN, SCRIPT_FUSE_DIR_TOKEN.replace('__FUSE_DIR__', self.to_fuse_dir)
            )
    
            # - are we configuring for mosaic pip?
            if self.with_mosaic_pip:
                script = script.replace(
                    SCRIPT_WITH_MOSAIC_TOKEN, SCRIPT_WITH_MOSAIC_TOKEN.replace('0','1')
                )
            
            # - are we configuring for gdal?
            if self.with_gdal:
                script = script.replace(
                    SCRIPT_WITH_GDAL_TOKEN, SCRIPT_WITH_GDAL_TOKEN.replace('0','1')
                )
            
            # - are we configuring for ubuntugis?
            if self.with_ubuntugis:
                script = script.replace(
                    SCRIPT_WITH_UBUNTUGIS_TOKEN, SCRIPT_WITH_UBUNTUGIS_TOKEN.replace('0','1')
                )
    
            # - set the mosaic version for pip
            pip_str=''
            if self.override_mosaic_version is not None:
                pip_str = f'=={self.override_mosaic_version}'
                if any(c in self.override_mosaic_version for c in ['=','<','<']):
                    pip_str = self.override_mosaic_version   
            elif mosaic_version is not None:
                pip_str = f'=={mosaic_version}'
            script = script.replace(
                SCRIPT_MOSAIC_PIP_VERSION_TOKEN, 
                SCRIPT_MOSAIC_PIP_VERSION_TOKEN.replace(
                    '__MOSAIC_PIP_VERSION__', pip_str)
            )
            
            # - write the configured init script
            with open(script_out_path, 'w') as file:
                file.write(script)
                
        # --- end of script config ---

        with_resources = self.jar_copy or self.jni_so_copy
        if with_resources:        
            # - handle jar copy
            if self.jar_copy:
                # url and version details
                GITHUB_RELEASE_URL_BASE = 'https://github.com/databrickslabs/mosaic/releases'
                resource_version = github_version
                if github_version is None:
                    latest = str(requests.get(f'{GITHUB_RELEASE_URL_BASE}/latest', allow_redirects=True).content)
                    resource_version = latest.split("/tag/v_")[1].split('"')[0]
                
                # download jar
                jar_filename = f'mosaic-{resource_version}-jar-with-dependencies.jar'
                jar_url = f'{GITHUB_RELEASE_URL_BASE}/download/v_{resource_version}/{jar_filename}'
                jar_request = requests.get(jar_url, allow_redirects=True, stream=True)
                with open(f'{self.to_fuse_dir}/{jar_filename}', 'wb') as jar_file:
                    shutil.copyfileobj(jar_request.raw, jar_file)
            
            # - handle so copy
            if self.jni_so_copy:
                for so_filename in ['libgdalalljni.so', 'libgdalalljni.so.30', 'libgdalalljni.so.30.0.3']:
                    so_url = f'{GITHUB_CONTENT_TAG_URL}/resources/gdal/jammy/{so_filename}'
                    so_request = requests.get(so_url, allow_redirects=True, stream=True)
                    with open(f'{self.to_fuse_dir}/{so_filename}', 'wb') as so_file:
                        shutil.copyfileobj(so_request.raw, so_file)

        # - echo status
        print(f"::: Install setup complete :::")
        print(f"- Settings: 'with_mosaic_pip'? {self.with_mosaic_pip}, 'with_gdal'? {self.with_gdal}, 'with_ubuntugis'? {self.with_ubuntugis}")
        print(f"            'override_mosaic_version'? {self.override_mosaic_version}, 'jar_copy'? {self.jar_copy}, 'jni_so_copy'? {self.jni_so_copy}")
        print(f"- Fuse Dir: '{self.to_fuse_dir}'")
        if with_script:
          print(f"- Init Script: configured and stored at '{self.script_out_name}'; ", end='')
          print(f"add to your cluster and restart,")
          print(f"               more at https://docs.databricks.com/en/init-scripts/cluster-scoped.html")
        if with_resources:
          print(f"- Resource(s): copied")
        print("\n")
    
def setup_fuse_install(
    to_fuse_dir: str, with_mosaic_pip: bool, with_gdal: bool, 
    with_ubuntugis: bool = False, script_out_name: str = 'mosaic-fuse-init.sh', 
    override_mosaic_version: str = None, jar_copy: bool = True, jni_so_copy: bool = True
) -> None:
    """
    [1] Copies Mosaic "fat" JAR (with dependencies) into `to_fuse_dir`
        - by default, version will match the current mosaic version executing the command,
          assuming it is a released version; if `override_mosaic_version` is a single value, 
          versus a range, that value will be used instead
        - this doesn't involve a script unless `with_mosaic_pip=True` or `with_gdal=True`
        - if `jar_copy=False`, then the JAR is not copied
    [2] if `with_mosaic_pip=True`
        - By default, configures script to pip install databricks-mosaic using current mosaic 
          version executing the command or to `override_mosaic_version`
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

    Parameters
    ----------
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
    script_out_name : str
            name of the script to be written;
            default is 'mosaic-fuse-init.sh'.
    override_mosaic_version: str
            String value to use to override the mosaic version to install,
            e.g. '==0.4.0' or '<0.5,>=0.4';
            default is None.
    jar_copy: bool
            Whether to copy the Mosaic JAR;
            default is True.
    jni_so_copy: bool
            Whether to copy the GDAL JNI shared objects;
            default is True.
    Returns
    -------
    """
    setup_mgr = SetupMgr(
        to_fuse_dir,
        with_mosaic_pip = with_mosaic_pip, 
        with_gdal = with_gdal,
        with_ubuntugis = with_ubuntugis, 
        script_out_name = script_out_name,
        override_mosaic_version = override_mosaic_version,
        jar_copy = jar_copy, 
        jni_so_copy = jni_so_copy
    )
    setup_mgr.configure()

     
def setup_gdal(
    to_fuse_dir: str = '/dbfs/FileStore/geospatial/mosaic/gdal/jammy',
    with_mosaic_pip: bool = False, with_ubuntugis: bool = False, 
    script_out_name: str = 'mosaic-gdal-init.sh',
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
    script_out_name : str
            name of the script to be written;
            default is 'mosaic-gdal-init.sh'.
    override_mosaic_version: str
            String value to use to override the mosaic version to install,
            e.g. '==0.4.0' or '<0.5,>=0.4';
            default is None.

    Returns
    -------
    """    
    setup_mgr = SetupMgr(
        to_fuse_dir,
        with_mosaic_pip = with_mosaic_pip, 
        with_ubuntugis = with_ubuntugis, 
        script_out_name = script_out_name,
        override_mosaic_version = override_mosaic_version,
    )
    setup_mgr.configure()


def enable_gdal(spark: SparkSession) -> None:
    """
    Enable GDAL at runtime on a cluster with GDAL installed using init script, 
    e.g.  generated by setup_gdal() or setup_fuse_install() call.

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
            "You can run setup_gdal() or setup_fuse_install() to generate the init script for install GDAL install.\n"
        )
        print(
            "After the init script is generated, you need to add the init script to your cluster and restart to complete the setup.\n"
        )
        print("Error: " + str(e))
