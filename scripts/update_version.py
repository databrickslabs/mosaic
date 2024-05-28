import os
import re
import sys

def main(version, snapshot):
    """
    TODO: Also need to adjust...
    [1] /R/sparklyr-mosaic/tests.R
        - e.g. "sparklyrMosaic_0.4.3.tar.gz"
    [2] /src/main/scala/com/databricks/labs/mosaic/functions/MosaicContext.scala
        - e.g. "mosaicVersion: String = "0.4.3"
    """
    update_pom_version('../pom.xml', version + snapshot)
    update_python_version('../python/mosaic/__init__.py', version + snapshot)
    update_r_version('../R/sparkR-mosaic/sparkrMosaic/DESCRIPTION', version)
    update_r_version('../R/sparklyr-mosaic/sparklyrMosaic/DESCRIPTION', version)

    if not snapshot:
        update_docs_version('../docs/source/conf.py', version)

def update_pom_version(file, version):
    update_version(file, r'(<version>)(\d+\.\d+\.\d+(-SNAPSHOT)?)(</version>)', r'\g<1>%s\g<4>' % version)

def update_python_version(file, version):
    update_version(file, r'(__version__ = )("\d+\.\d+\.\d+(-SNAPSHOT)?")', r'\g<1>"%s"' % version)

def update_r_version(file, version):
    update_version(file, r'(Version: )(\d+\.\d+\.\d+(-SNAPSHOT)?)', r'\g<1>%s' % version)

def update_docs_version(file, version):
    update_version(file, r'(release = )("v\d+\.\d+\.\d+")', r'\g<1>"v%s"' % version)


def update_version(file, pattern, replace_pattern):
    #Open file and replace
    
    with open (file, 'r' ) as f:
        content = f.read()
        content_new = re.sub(pattern, replace_pattern, content, flags = re.M, count = 1)
    with open (file, 'w' ) as f:
        f.write(content_new)


if __name__ == "__main__":

    version = sys.argv[1]
    snapshot = ""
    if len(sys.argv) > 2:
        snapshot = sys.argv[2]
        if snapshot != "-SNAPSHOT":
            raise Exception("Invalid argument: %s. Use -SNAPSHOT or leave blank." % snapshot)

    if re.match(r'\d+\.\d+\.\d+', version) is None:
        raise Exception("Invalid version: %s. Use format x.y.z." % version)
    
    os.system(
        "git checkout main && git pull && git checkout -b releases/v_%s%s" % (version, snapshot)
    )
    main(version, snapshot)
    print("Version updated to %s%s" % (version, snapshot))
    print("Please update the CHANGELOG.md and press ENTER to continue.")
    input()

    os.system("git add '../pom.xml'")
    os.system("git add '../python/mosaic/__init__.py'")
    os.system("git add '../R/sparkR-mosaic/sparkrMosaic/DESCRIPTION'")
    os.system("git add '../R/sparklyr-mosaic/sparklyrMosaic/DESCRIPTION'")

    os.system("git commit -m 'Update version to %s%s' && git push -u origin releases/v_%s%s" % (version, snapshot, version, snapshot))
