import os
from dataclasses import dataclass
from pathlib import Path

import requests

__all__ = ["SetupMgr", "setup_fuse_install"]


@dataclass
class SetupMgr:
    """
    Defaults mirror setup_gdal.
    """

    to_fuse_dir: str
    script_in_name: str = "mosaic-gdal-init.sh"
    script_out_name: str = "mosaic-gdal-init.sh"
    jar_copy: bool = False
    jni_so_copy: bool = False

    def configure(self, test_mode: bool = False) -> bool:
        """
        Handle various config options.
        Returns True unless resources fail to download.
        """
        # - generate fuse dir path
        #   volumes must be pre-generated in unity catalog
        os.makedirs(self.to_fuse_dir, exist_ok=True)

        # - start with the un-configured script (from repo)
        #   this is using a different (repo) folder in 0.4.2+ (to allow prior versions to work)
        GITHUB_CONTENT_TAG_URL = (
            "https://raw.githubusercontent.com/databrickslabs/mosaic/main"
        )
        script_url = f"{GITHUB_CONTENT_TAG_URL}/scripts/0.4.2/{self.script_in_name}"
        script = None
        root_path = None
        if not test_mode:
            with requests.Session() as s:
                script = s.get(script_url, allow_redirects=True).text
        else:
            # test_mode (use local resource)
            # - up 4 parents [0..3]
            # - api [0] -> mosaic [1] -> python [2] -> mosaic [3]
            root_path = Path(__file__).parents[3]
            script_path = root_path / "scripts" / "0.4.2" / self.script_in_name
            script = script_path.read_text(encoding="utf-8")

        # - tokens used in script
        SCRIPT_FUSE_DIR_TOKEN = "FUSE_DIR='__FUSE_DIR__'"  # <- ' added
        SCRIPT_WITH_FUSE_SO_TOKEN = "WITH_FUSE_SO=0"

        # - set the fuse dir
        script = script.replace(
            SCRIPT_FUSE_DIR_TOKEN,
            SCRIPT_FUSE_DIR_TOKEN.replace("__FUSE_DIR__", self.to_fuse_dir),
        )

        # - are we configuring for jni so copy?
        if self.jni_so_copy:
            script = script.replace(
                SCRIPT_WITH_FUSE_SO_TOKEN,
                SCRIPT_WITH_FUSE_SO_TOKEN.replace("0", "1"),
            )

        # - write the configured init script
        script_out_path = Path(self.to_fuse_dir) / self.script_out_name
        script_out_path.write_text(script, encoding="utf-8")

        # --- end of script config ---

        with_resources = self.jar_copy or self.jni_so_copy
        resource_statuses = {}
        if with_resources:
            CHUNK_SIZE = 1024 * 1024 * 64  # 64MB
            # - handle jar copy
            #   0.4.2 always get the latest release
            if self.jar_copy:
                if not test_mode:
                    # url and version details
                    GITHUB_RELEASE_URL_BASE = (
                        "https://github.com/databrickslabs/mosaic/releases"
                    )
                    latest = None
                    with requests.Session() as s:
                        latest = str(
                            s.get(
                                f"{GITHUB_RELEASE_URL_BASE}/latest",
                                allow_redirects=True,
                            ).content
                        )
                    resource_version = latest.split("/tag/v_")[1].split('"')[0]
                    # download jar
                    jar_filename = (
                        f"mosaic-{resource_version}-jar-with-dependencies.jar"
                    )
                    jar_path = f"{self.to_fuse_dir}/{jar_filename}"
                    with requests.Session() as s:
                        r = s.get(
                            f"{GITHUB_RELEASE_URL_BASE}/download/v_{resource_version}/{jar_filename}",
                            stream=True,
                        )
                        with open(jar_path, "wb") as f:
                            for ch in r.iter_content(chunk_size=CHUNK_SIZE):
                                f.write(ch)
                        resource_statuses[jar_filename] = r.status_code
                else:
                    # test_mode (use local resources)
                    lib_path = root_path / "python" / "mosaic" / "lib"
                    src_jar_path = None
                    for p in lib_path.iterdir():
                        if p.name.startswith("mosaic-") and p.name.endswith(
                            "-jar-with-dependencies.jar"
                        ):
                            src_jar_path = p
                            break
                    if src_jar_path:
                        dst_jar_path = Path(f"{self.to_fuse_dir}/{src_jar_path.name}")
                        dst_jar_path.write_bytes(src_jar_path.read_bytes())

            # - handle so copy
            if self.jni_so_copy:
                so_names = [
                    "libgdalalljni.so",
                    "libgdalalljni.so.30",
                    "libgdalalljni.so.30.0.3",
                ]
                if not test_mode:
                    with requests.Session() as s:
                        for so_filename in so_names:
                            so_path = f"{self.to_fuse_dir}/{so_filename}"
                            r = s.get(
                                f"{GITHUB_CONTENT_TAG_URL}/resources/gdal/jammy/{so_filename}",
                                stream=True,
                            )
                            with open(so_path, "wb") as f:
                                for ch in r.iter_content(chunk_size=CHUNK_SIZE):
                                    f.write(ch)
                            resource_statuses[so_filename] = r.status_code
                else:
                    # test_mode (use local resources)
                    resources_path = root_path / "resources" / "gdal" / "jammy"
                    for so_filename in so_names:
                        src_so_path = resources_path / so_filename
                        dst_so_path = Path(f"{self.to_fuse_dir}/{so_filename}")
                        dst_so_path.write_bytes(src_so_path.read_bytes())

        # - echo status
        print(f"::: Install setup complete :::")
        print(
            f"- Settings: 'jar_copy'? {self.jar_copy}, 'jni_so_copy'? {self.jni_so_copy}"
        )
        print(f"- Fuse Dir: '{self.to_fuse_dir}'")
        print(
            f"- Init Script: configured and stored at '{self.script_out_name}'; ",
            end="",
        )
        print(f"add to your cluster and restart,")
        print(
            f"               more at https://docs.databricks.com/en/init-scripts/cluster-scoped.html"
        )
        if with_resources:
            print(f"- Resource(s): copied")
            print(resource_statuses)
        if test_mode:
            print(f"- Test Mode files generated")
            print(os.listdir(self.to_fuse_dir))
        print("\n")

        if not any(resource_statuses) or all(
            value == 200 for value in resource_statuses.values()
        ):
            return True
        else:
            return False


def setup_fuse_install(
    to_fuse_dir: str,
    script_out_name: str = "mosaic-fuse-init.sh",
    jar_copy: bool = True,
    jni_so_copy: bool = True,
    test_mode: bool = False,
) -> bool:
    """
    [1]  if `jar_copy=True`
         - Copies current mosaic "fat" JAR (with dependencies) into `to_fuse_dir`
    [2] if `jni_so_copy=True`
        - configures to load shared objects from fuse dir (vs wget)
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
    script_out_name : str
            name of the script to be written;
            default is 'mosaic-fuse-init.sh'.
    jar_copy: bool
            Whether to copy the Mosaic JAR;
            default is True.
    jni_so_copy: bool
            Whether to copy the GDAL JNI shared objects;
            default is True.
    test_mode: bool
            Only for unit tests.
    Returns True unless resources fail to download.
    -------
    """
    setup_mgr = SetupMgr(
        to_fuse_dir,
        script_out_name=script_out_name,
        jar_copy=jar_copy,
        jni_so_copy=jni_so_copy,
    )
    return setup_mgr.configure(test_mode=test_mode)
