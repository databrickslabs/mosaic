package com.databricks.geotools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

import org.geotools.jdbc.JDBCDataStore;

public class LayersList {
	private long lastRefreshed = 0; 
	private Map<String, TableInfo> tableNames; 
	private static Logger LOGGER = org.geotools.util.logging.Logging.getLogger(LayersList.class);
	
	public LayersList() {}
	
	public class TableInfo {
		protected String schemaName;
		protected String tableName;
		protected String mosaicIndexColumn;
		protected int mosaicIndexResolution;
		public String getSchemaName() {
			return schemaName;
		}
		public String getTableName() {
			return tableName;
		}
		public String getMosaicIndexColumn() {
			return mosaicIndexColumn;
		}
		public int getMosaicIndexResolution() {
			return mosaicIndexResolution;
		}
		
		
	}
	
	public synchronized boolean includeTable(
			String schemaName, 
			String tableName, 
			String metadataTableName,
			JDBCDataStore dataStore,
			Connection cx,
			int refreshPeriod
			) throws SQLException {
		LOGGER.fine("Checking whether to include table: " + schemaName + "." + tableName);

		if (tableName.equals(metadataTableName)) {
			LOGGER.fine("Not including table " + schemaName + "." + tableName);
			return false;
		}

		String tableKey = schemaName + "." + tableName;
		
		boolean include = false;
		boolean needsRefresh = false;
		boolean initialize = false;
		long now = System.currentTimeMillis();
		try {

//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " acquiring read lock");
//			rwLock.readLock().lock();
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " read lock aquired");

			needsRefresh = now - lastRefreshed > refreshPeriod * 60 * 1000;
			initialize = tableNames == null;
		}
		catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		finally {
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " releasing read lock");
//			rwLock.readLock().unlock();
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " released read lock");
		}
		if (initialize || needsRefresh) {
				_rebuildLayers(cx, dataStore, metadataTableName, now);
			}
		
		if (tableNames.get(tableKey) == null) {
			LOGGER.fine("Not including table " + tableKey);
			include = false;
		}
		else {
			LOGGER.fine("Including table " + tableKey);
			include = true;
		}
		return include;
	}

	public TableInfo getTableInfo(String schemaName, String tableName) {
		return tableNames.get(schemaName + "." + tableName);
	}
	
	private void _rebuildLayers(Connection cx, JDBCDataStore dataStore, String metadataTableName, long now) throws SQLException {

		try {

//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " acquiring write lock");
//			rwLock.writeLock().lock();
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " write lock aquired");

			LOGGER.fine("Rebuilding list of tables to include...");
			String sql = "SELECT * from " + dataStore.getDatabaseSchema() + "." + metadataTableName + ";";
			LOGGER.fine("SQL: " + sql);
			Statement st = cx.createStatement();
			ResultSet rs = st.executeQuery(sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			//get the column type
			int columnCount = rsmd.getColumnCount();
			LOGGER.fine("ResultSet Column Count: " + columnCount);

			tableNames = new HashMap<String, TableInfo>();
			LOGGER.fine("created tableNames");
			while (rs.next()) {
				TableInfo tinfo = new TableInfo();
				tinfo.schemaName = rs.getString("table_schema");
				tinfo.tableName = rs.getString("table_name");
				try {
					tinfo.mosaicIndexColumn = rs.getString("mosaic_idx_col");
					tinfo.mosaicIndexResolution = rs.getInt("mosaic_idx_resolution");
				}
				catch (SQLException ex) {
					//eat the exception, those columns might not exist
				}

				
				String tableName = tinfo.schemaName + "." + tinfo.tableName;
				LOGGER.fine("Adding table " + tableName + " to list of tables to include..." );
				LOGGER.fine("schemaName:" + tinfo.schemaName);
				LOGGER.fine("tableName: " + tinfo.tableName);
				LOGGER.fine("mosaicIndexColumn: " + tinfo.mosaicIndexColumn);
				LOGGER.fine("mosaicIndexResolution:" + tinfo.mosaicIndexResolution);
				tableNames.put(tableName, tinfo);
			}
			lastRefreshed = now;
		}
		catch(Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
		finally {
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " releasing write lock");
//			rwLock.writeLock().unlock();
//			LOGGER.fine("Thread " + Thread.currentThread().getId() + " write lock released");
		}
	}

}
