# databricks-geotools-plugin

The Databricks Geotools plugin provides an implementation of the Geotools org.geotools.data.DataStoreFactorySpi to allow Geoserver to retrieve data from Databricks.  

Built/tested against the following Geoserver/Geotools versions:

Geoserver Version: 2.17.3 
Geotools Version: 23.3

Tested against Geoserver 2.20.2

## Usage

### Build the Connector Jar 
1.  Build the jar file via the maven package goal. 

2.  Copy the resulting jar file to the \[GEOTOOLS_INSTALL_DIR\]/webapps/geotools/WEB-INF/lib directory along with the [Databricks JDBC Driver](https://databricks.com/spark/jdbc-drivers-download) jar file.

NOTE:  This plugin requires version 2.6.25 of the Databricks JDBC driver or higher.  The plugin will not be able to connect to Databricks with lower versions.

3.  Restart Geoserver.

### Configure your Databricks Cluster

The Databricks Geotools Plugin currently requires the databricks cluster to have Apache Sedona installed and configured for a Pure SQL environment according to [these](https://sedona.incubator.apache.org/setup/databricks/) instructions.  This has been tested with Databricks Runtime 10.0 and above.

Additionally, for data engineering, you may want to install the following useful libraries as Cluster-level libraries:

- org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.1-incubating (Maven)
- org.datasyslab:geotools-wrapper:1.1.0-25.2 (Maven)
- sedona (PyPI)
- shapely (PyPI)
- apache-sedona\[spark\] (PyPI)
- h3 (PyPI) (provided by Mosaic, JAR by DBR)
- geospark (PyPI) (provided by DBR)
- geopandas (PyPI) (Optional)

For instructions on how to install libraries on your cluster, see the [documentation here](https://docs.databricks.com/libraries/index.html).

NOTE:  Autoscaling your cluster can negatively impact the performance of GeoServer, as the time to scale up/down and acquire nodes will cause the JDBC calls to block while the cluster scales up/down.  A performance/cost savings strategy would be to use a cron job to set the minimum/maximum cluster nodes to the same (higher) number during periods of frequent use, and the minimum number to be lower during periods of infrequent use.  Use the Databricks REST API for these calls.

### Databricks Data Engineering tasks

Use Databricks Mosaic, Carto, Apache Sedona, shapely, or other geospatial libraries to create bronze/silver/gold tables containing your geospatial layers.  There are a few requirements for data to be exposed via the connector:

- There must be one column of geometry (or one of its subclasses).  IMPORTANT: If using Mosaic or Carto, this column must be a binary column containing the Well-Known Binary represenation of the geometry.  If using Apache Sedona, this must be of type Geometry (A Sedona-defined UDT).
- There must be one sortable column to be used as the unique id.  It is recommended that this column be a long integer, however strings will work as well, at the expense of sorting performance.  IMPORTANT: For GeoTools-specific reasons, this column must NOT be named 'OBJECTID'.
- Finally, there must be a table that conforms to [this](https://docs.geotools.org/latest/javadocs/org/geotools/jdbc/MetadataTablePrimaryKeyFinder.html) schema present in the schema/database.  By default this table is named 'gt_pk_metadata', although this is configurable in the Databricks Geotools Plugin configuration page in GeoServer.  Every table containing layer data must have an entry in this table.  Only the database tables with an entry in this table will be considered as a source of  layer data in GeoServer; the rest will not show up in the layer publishing UI.


NOTE: We extended this schema to include the following columns: 
- a column called 'srid' to specify the coordinate system of the layer.  You must populate this field.  Currently this must be set to 4326.
- a column called 'mosaic_idx_col'.  If you are using the Databricks Mosaic library as your geometry library for the geoserver tables, and you want to use index-accelerated st_contains and st_intersects operations,  you must set this to whatever column contains the output of mosaicfill([geometry column]).  Otherwise, set this to null.
- a column called 'mosaic_idx_resolution'.  If you are using the Databricks Mosaic library as your geometry library for the geoserver tables, and you want to use index-accelerated st_contains and st_intersects operations, this column must be set to the value of the 'resolution' parameter passed into mosaicfill above.  Otherwise, set this column to null.

Typically an entry like the following is sufficient:

```
table_schema (string): database schema name
table_name (string): layer table name
pk_column (string): name of the sortable primary key column
pk_column_idx (int): Typically set to null
pk_policy (string): set to 'assigned'
pk_sequence (string): set to 'null'
srid (string): set to 4326 (for WGS-84)
mosaic_idx_col: set to [index column name] or null if no mosaic or no index
mosaic_idx_resolution: set to [integer resolution] or null if no mosaic or no index
```

### Configure the Databricks Data Store in Geoserver

1.  Log in to the Geoserver admin UI as the geoserver admin user.

2.  Navigate to the Stores link on the left hand side and click on "Add new Store" at the top of the page.  Select "Databricks" from the list of choices.

3.  Pick a workspace for the datastore to belong to.  The purpose of Geoserver workspaces are to logically group data that is related in some way.  You may want to create a new workspace for the Databricks data to belong to, or you can use an existing workspace.

4.  Use the information in your cluster's Advanced->JDBC/ODBC configuration page to fill out the values on this page.  By default, the "Additional JDBC Parameters" param is set to the following string--please review it and make sure that is correct for your environment:

```
1;ServerCertificate=false;encrypt=true;trustServerCertificate=true
```

Set 'Authentication Mechanism' to 3.  The "Minutes Between Refreshing Layer List" specifies how long the server will wait before re-reading the gt_pk_metadata table mentioned above.  This can be an expensive operation so we do not want to be constantly refreshing this list.

5.  When the values are filled out, click 'Save'.  Note that if your Databricks cluster has not yet been started, it may take some time for the Save to complete since it will attempt to contact the database to ensure connectivity.  It is recommended to have your cluster up and running before configuring this connection.


Once you have completed the above steps, you should be able to add new layers.  Be sure to set the Lat/Lon Bounding box of each layer by pressing the 'Compute from native bounds' button on the layer publishing page.  

Recommendations:
- It is recommended that your cluster be running before any queries are made to from Geoserver to the cluster.  Otherwise, Databricks will attempt to start your JDBC cluster, which may cause the JDBC request from GeoServer to time out and potentially return errors to your Geoserver client.  This can be avoided by increasing the JDBC request timeout value in the Datastore configuration page.
- Experiment with the store's fetch size parameter to find the optimal value, especially if you are using an index-accelerated Mosaic table in Databricks.  It's likely that higher values (10000) may yield better performance.


