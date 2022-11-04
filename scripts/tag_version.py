import os
import sys
import re

def main(version):
    os.system("git checkout main && git pull")
    os.system("git tag -a v_%s -m 'Version %s'" % (version, version))
    os.system("git push origin v_%s" % version)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise Exception("Please specify a version. Use format x.y.z.")
    version = sys.argv[1]

    if (not re.match(r'\d+\.\d+\.\d+', version)):
        raise Exception("Invalid version: %s. Use format x.y.z." % version)

    main(version)

