/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2015, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package com.databricks.geotools;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.logging.Logger;

import org.geotools.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.KVP;


/**
 * DataStoreFactory for Databricks Lakehouse.
 *
 * @author Tom Diepenbrock <tom.diepenbrock@databricks.com>
 */
public class DatabricksDataStoreFactory extends JDBCDataStoreFactory {
	public static final Param DATABRICKS_IMPL = 
    		new Param(
	            "Databricks Library",
	            DatabricksGISLibrary.class,
	            "Backend Databricks Library",
	            true,
	            DatabricksGISLibrary.SEDONA,
	            new KVP(
	                    Param.OPTIONS,
	                    Arrays.asList(DatabricksGISLibrary.values())));
	
    /** parameter for database type */
    public static final Param DBTYPE =
            new Param(
                    "dbtype",
                    String.class,
                    "Type",
                    true,
                    "databricks",
                    Collections.singletonMap(Parameter.LEVEL, "program"));

    /** Defaults for Databricks JDBC */
    public static final Param TABLES_REFRESH_TIME =
            new Param(
                    "Minutes Between Refreshing Layer Table List",
                    Integer.class,
                    "Minutes Between Refreshing Layer Table List",
                    true,
                    10);

    public static final Param HTTP_PATH =
            new Param(
                    "Http Path",
                    String.class,
                    "Http Path",
                    true,
                    "");
    
    public static final Param DRIVER_LOG_LEVEL =
          new Param(
                  "Log Level",
                  Integer.class,
                  "Logging level.  Log Level 0 = off; Log Level 6 = all activity",
                  false,
                  "");
    
    public static final Param DRIVER_LOG_PATH =
            new Param(
                    "Log File Path",
                    String.class,
                    "Directory where Simba JDBC log file will be placed",
                    false,
                    "/tmp");
    
    public static final Param TRANSPORT_MODE =
            new Param(
                    "Transport mode",
                    String.class,
                    "Transport mode.  Set to 'http'.",
                    true,
                    "http");
    
    public static final Param SSL =
            new Param(
                    "SSL",
                    String.class,
                    "SSL mode.  Set to '1'.",
                    true,
                    "1");
    
    public static final Param AUTH_MECH =
            new Param(
                    "Authentication Mechanism",
                    String.class,
                    "Authentication Mechanism.  Set to '3'.",
                    true,
                    "3");
    
    public static final Param ADDITIONAL_JDBC_PARAMS =
            new Param(
                    "Additional JDBC Parameters",
                    String.class,
                    "Additional JDBC Parameters.",
                    false,
                    "ServerCertificate=false;encrypt=true;trustServerCertificate=true");
    
    
    
    
    private static final String DRIVER_CLASS_NAME = "com.databricks.client.jdbc.Driver";

	private Logger LOGGER;
	private String metadataTableName;
	private Integer tablesRefreshTime;
	private String databricksImplClass;

    public DatabricksDataStoreFactory() {
    	super();
    	this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
    }
    
    public String getMetadataTableName() { return metadataTableName; }
    
    public int getTablesRefreshTime() {
    	if (tablesRefreshTime == null) {
    		tablesRefreshTime = new Integer(10);
    	}
    	return tablesRefreshTime.intValue();
    }
    
    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore, Map params) {
    	try {
    	Object databricksImplClazz = DATABRICKS_IMPL.lookUp(params);
    	LOGGER.info("DATABRICKS_IMPL = " + databricksImplClazz);
    	databricksImplClass = (String)((DatabricksGISLibrary)DATABRICKS_IMPL.lookUp(params)).value;
    	return createSQLDialect(dataStore);
    	}
    	catch(IOException ex) {
    		throw new RuntimeException(ex);
    	}
    }
    
    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
    	LOGGER.fine("creating SQL Dialect");
    	
    	try {
    		LOGGER.info("looking for class for className " + databricksImplClass);
	    	Class<?> clazz = Class.forName(databricksImplClass);
	    	Constructor<?> ctor = clazz.getConstructor(JDBCDataStore.class, Integer.class);
	    	Object object = ctor.newInstance(new Object[] { dataStore, getTablesRefreshTime() });
	    	
	    	DatabricksSQLDialect dd = (DatabricksSQLDialect)object;
	    	
	    	//unfortunately, metadataTableName is null at this point in the lifecycle.
	    	//Later versions of GeoTools will pass a params Map in to this function and you
	    	//can get the metadata table name out of the params, but this forces us to a higher
	    	//version of GeoTools than we want to support.  See the workaround below.
	    	dd.setMetadataTableName(metadataTableName);
	    	
	        return dd;
    	}
    	catch (Exception ex) {
    		throw new RuntimeException(ex);
    	}
    }

    @Override
// the below method signature only works for later versions of geotools
//    protected String getJDBCUrl(Map<String, ?> params) throws IOException {
    protected String getJDBCUrl(Map params) throws IOException {
    	Integer logLevel = (Integer) DRIVER_LOG_LEVEL.lookUp(params);
    	String logPath = (String) DRIVER_LOG_PATH.lookUp(params);

    	String user = (String) USER.lookUp(params);
    	String pwd = (String) PASSWD.lookUp(params);
    	String httpPath = (String) HTTP_PATH.lookUp(params);
    	String authMech = (String) AUTH_MECH.lookUp(params);
    	String sslFlag = (String)SSL.lookUp(params);
    	String transportMode = (String)TRANSPORT_MODE.lookUp(params);
    	String additionalJDBCParams = (String)ADDITIONAL_JDBC_PARAMS.lookUp(params);
    	
    	//LOGGER.fine("additionalJDBCParams: " + additionalJDBCParams);
    	
    	StringBuilder jdbcUrlBuilder = new StringBuilder();
    	jdbcUrlBuilder.append(super.getJDBCUrl(params))
    		.append(";transportMode=" + transportMode)
    		.append(";ssl=" + sslFlag)
    		.append(";httpPath=" + httpPath)
    		.append(";AuthMech=" + authMech)
    		.append(";UID=" + user)
    		.append(";PWD=" + pwd);
    	
    	if (additionalJDBCParams != null && additionalJDBCParams.length() > 0)
    		jdbcUrlBuilder.append(";" + additionalJDBCParams);
    	
    	if (logLevel != null && logPath != null ) {
    		jdbcUrlBuilder.append(";LogLevel=" + logLevel + ";LogPath=" + logPath);
    	}
    	
    	String jdbcUrl = jdbcUrlBuilder.toString();

    	LOGGER.fine("JDBC Url: " + jdbcUrl);
    	return jdbcUrl;
    }
    
    @Override
    public String getDisplayName() {
        return "Databricks";
    }

    @Override
    protected String getDriverClassName() {
    	LOGGER.fine("getDriverClassName() called; returning: " + DRIVER_CLASS_NAME);
        return DRIVER_CLASS_NAME;
    }

    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    @Override
    public String getDescription() {
        return "Connect to a Databricks Lakehouse cloud store";
    }

    @Override
    protected String getValidationQuery() {
        return "select now()";
    }

    @Override
//    protected void setupParameters(Map<String, Object> parameters) {
    protected void setupParameters(Map parameters) {

        parameters.put(NAMESPACE.key, NAMESPACE);
    	parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(HOST.key, HOST);
        parameters.put(PORT.key, PORT);
        
        parameters.put(HTTP_PATH.key, HTTP_PATH);
        parameters.put(TRANSPORT_MODE.key, TRANSPORT_MODE);
        parameters.put(SSL.key, SSL);
        parameters.put(AUTH_MECH.key, AUTH_MECH);
        
        parameters.put(DATABASE.key, DATABASE);
        parameters.put(SCHEMA.key, SCHEMA);
        parameters.put(USER.key, USER);
        parameters.put(PASSWD.key, PASSWD);
        parameters.put(ADDITIONAL_JDBC_PARAMS.key, ADDITIONAL_JDBC_PARAMS);

        parameters.put(DATABRICKS_IMPL.key, DATABRICKS_IMPL);
        
        parameters.put(EXPOSE_PK.key, EXPOSE_PK);
        parameters.put(MAXCONN.key, MAXCONN);
        parameters.put(MINCONN.key, MINCONN);
        parameters.put(FETCHSIZE.key, FETCHSIZE);
        parameters.put(BATCH_INSERT_SIZE.key, BATCH_INSERT_SIZE);
        parameters.put(MAXWAIT.key, MAXWAIT);
        if (getValidationQuery() != null) parameters.put(VALIDATECONN.key, VALIDATECONN);
        parameters.put(TEST_WHILE_IDLE.key, TEST_WHILE_IDLE);
        parameters.put(TIME_BETWEEN_EVICTOR_RUNS.key, TIME_BETWEEN_EVICTOR_RUNS);
        parameters.put(MIN_EVICTABLE_TIME.key, MIN_EVICTABLE_TIME);
        parameters.put(EVICTOR_TESTS_PER_RUN.key, EVICTOR_TESTS_PER_RUN);
        parameters.put(PK_METADATA_TABLE.key, PK_METADATA_TABLE);
        parameters.put(TABLES_REFRESH_TIME.key, TABLES_REFRESH_TIME);
        parameters.put(SQL_ON_BORROW.key, SQL_ON_BORROW);
        parameters.put(SQL_ON_RELEASE.key, SQL_ON_RELEASE);
        parameters.put(CALLBACK_FACTORY.key, CALLBACK_FACTORY);
        
        parameters.put(DRIVER_LOG_LEVEL.key, DRIVER_LOG_LEVEL);
        parameters.put(DRIVER_LOG_PATH.key, DRIVER_LOG_PATH);

    }

//    @Override
//    protected String getJDBCUrl(Map<String, ?> params) throws IOException {
//        return ((String) CONN_STRING.lookUp(params));
//    }

    
    @Override
//    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map<String,?> params)
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
            throws IOException {
    	LOGGER.fine("DatabricksDataStoreFactory: createDataStoreInternal() entered");
        dataStore.setAssociations(false);
        
    	metadataTableName =  (String)PK_METADATA_TABLE.lookUp(params);
    	tablesRefreshTime = (Integer)TABLES_REFRESH_TIME.lookUp(params);
    	
    	
        return dataStore;
    }
}
