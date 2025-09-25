For tests to run in docker we need to copy the jar to site-packages/pyspark/jars.
Example: /root/mosaic/python/dev/lib/python3.12/site-packages/pyspark/jars/
spark.addArtifact does not work due to permissions issues.
Also there could be annoying warnings about jypiter_client which isnt something relevant to the pytest we do.
If those warnings occur use:  export JUPYTER_PLATFORM_DIRS=1