package com.databricks.geotools;

import java.sql.Connection;
import java.sql.SQLException;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.PreparedStatementSQLDialect;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;

public abstract class DatabricksSQLDialect extends PreparedStatementSQLDialect {

	
	protected String metadataTableName = null;
	protected WKTReader wktReader = null;
	
	//WKBReader is not thread-safe, the WKBReader.read() method will throw exceptions
	//randomly unless it is accessed via a ThreadLocal
	protected ThreadLocal<WKBReader> wkbReader = null;
	protected int refreshPeriod;
	protected LayersList layersList = null;
	
	private static String GT_PK_METADATA = "GT_PK_METADATA";
	
	public void setMetadataTableName(String tableName) {
		LOGGER.fine("tableName = " + tableName);
    	if (tableName == null) 
    		this.metadataTableName = GT_PK_METADATA;
    	else
    		this.metadataTableName = tableName;
    }
	
	public DatabricksSQLDialect(JDBCDataStore dataStore, Integer refreshPeriod) {
		super(dataStore);
		this.wktReader = new WKTReader(new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326));
    	this.wkbReader = ThreadLocal.withInitial(() -> new WKBReader(new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)));
    	this.metadataTableName = GT_PK_METADATA;
    	this.refreshPeriod = refreshPeriod;
    	this.layersList = new LayersList();
	}
	
	@Override
	public boolean includeTable(String schemaName, String tableName, Connection cx)
            throws SQLException {
		String mtn = ((DatabricksDataStoreFactory)this.dataStore.getDataStoreFactory()).getMetadataTableName();
		if (mtn == null) mtn = GT_PK_METADATA;
		
		return layersList.includeTable(schemaName, tableName, mtn, dataStore, cx, refreshPeriod);
        
    }
	
	public LayersList getLayersList() {return layersList;}
	
}
