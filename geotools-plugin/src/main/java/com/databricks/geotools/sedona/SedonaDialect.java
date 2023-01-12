package com.databricks.geotools.sedona;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.geotools.data.Query;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.PreparedFilterToSQL;
import org.geotools.jdbc.PreparedStatementSQLDialect;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;

import com.databricks.geotools.DatabricksSQLDialect;
import com.databricks.geotools.LayersList;


/**
 * SedonaDialect for Databricks Lakehouse.
 *
 * @author Tom Diepenbrock <tom.diepenbrock@databricks.com>
 */
public class SedonaDialect extends DatabricksSQLDialect {


	private Logger LOGGER;
	
	
    public SedonaDialect(JDBCDataStore dataStore, Integer refreshPeriod) {
        super(dataStore, refreshPeriod);
    	this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
    }

    
    
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
    	LOGGER.fine("in registerSqlTypeNameToClassMappings()");
        super.registerSqlTypeNameToClassMappings(mappings);
        
        mappings.put("ARRAY<TINYINT>", Geometry.class);
    }
    
    @Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {
    	LOGGER.fine("in getMapping()");
    	String columnName = columnMetaData.getString("COLUMN_NAME");
    	String typeName = columnMetaData.getString("TYPE_NAME");
    	int dataType = columnMetaData.getInt("DATA_TYPE");
    	String nullable = columnMetaData.getString("IS_NULLABLE");
    	
        LOGGER.fine("column name: " + columnName);
        LOGGER.fine("column type: " + typeName);
        LOGGER.fine("column dataType: " + dataType);
        LOGGER.fine("column nullable: " + nullable);

        if (typeName.equals("ARRAY<TINYINT>") && dataType == 12) {
        	LOGGER.fine("FOUND GEOMETRY COLUMN: " + columnName);
        	return Geometry.class;
        }
        Class<?> c = super.getMapping(columnMetaData, cx);
        if (c != null)
        	LOGGER.fine("getMapping() returning " + c.getName());
        else
        	LOGGER.fine("getMapping() returning <null>"); 
        return c;
    }
    
    @Override
    public void initializeConnection(Connection cx) throws SQLException {
    	LOGGER.fine("DatabricksDialect: initalizeConnect() entered");
        super.initializeConnection(cx);
    }

    @Override
    public Integer getGeometrySRID(String schemaName,
            String tableName,
            String columnName,
            Connection cx)
     throws SQLException {
    	//hardcoded to 4326 for now
    	return 4326;
    }
    
    @Override
    public List<ReferencedEnvelope> getOptimizedBounds(String schema,
            SimpleFeatureType featureType,
            Connection cx)
     throws SQLException,
            IOException {
    	LOGGER.fine("in getOptimizedBounds()");
    	LOGGER.fine("schema = " + schema);
    	LOGGER.fine("featureType = " + featureType.getTypeName());
    	
    	Statement st = null;
        ResultSet rs = null;

        String geometryColumnName = featureType.getGeometryDescriptor().getLocalName();
        //String sql = "SELECT min_x, min_y, max_x, max_y FROM " + schema + ".GT_PK_METADATA WHERE table_schema = '" + schema + "' and table_name = '" + featureType.getTypeName() + "'";
        String sql = "SELECT ST_Envelope_Aggr(" + geometryColumnName + ") as envelope from " + schema + "." + featureType.getTypeName();
        
        LOGGER.fine("sql: " + sql);
        st = cx.createStatement();
        rs = st.executeQuery(sql);
        
    	ArrayList<ReferencedEnvelope> list = new ArrayList<ReferencedEnvelope>();
        if (!rs.next()) {
        	LOGGER.fine("no information found for layer " + featureType.getTypeName() + ", returning bounds = everything");
        	list.add(ReferencedEnvelope.EVERYTHING);
        	return list;
        }
//        
//        double minX = rs.getDouble("min_x");
//        double minY = rs.getDouble("min_y");
//        double maxX = rs.getDouble("max_x");
//        double maxY = rs.getDouble("max_y");
        
//        list.add(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84));
        
        String value = rs.getString(1);
//      LOGGER.fine("value: " + value);
      
	      try {
	      	Geometry g = wktReader.read(value);
	      	Envelope e = g.getEnvelopeInternal();
	      	list.add(new ReferencedEnvelope(e, DefaultGeographicCRS.WGS84));
	      }
	      catch (ParseException ex) {
	      	ex.printStackTrace();
	      	throw new IOException(ex);
	      }
    	 	
    	rs.close();
    	return list;
    }
    
    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        LOGGER.fine("in encodeGeometryEnvelope() with sql = " + sql);

    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx)
            throws SQLException, IOException {
    	LOGGER.fine("in decodeGeometryEnvelope!!");
        return null;
    }

    @Override
    public Geometry decodeGeometryValue(
            GeometryDescriptor descriptor,
            ResultSet rs,
            String column,
            GeometryFactory factory,
            Connection cx,
            Hints hints)
            throws IOException, SQLException {
//        LOGGER.fine("in decodeGeometryValue()");
        String value = rs.getString(column);
//        LOGGER.fine("value: " + value);
        
        try {
        	return wktReader.read(value);
        }
        catch (ParseException ex) {
        	ex.printStackTrace();
        	throw new IOException(ex);
        }
        
    }

//	@Override
//	public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
//		// TODO Auto-generated method stub
//		
//	}
	
	@Override
	public String getNameEscape() {
        return "";
    }
	
//	@Override
//	public void encodeSchemaName(String raw, StringBuffer sql) {
//        sql.append(raw);
//    }
	
	@Override
	public boolean isAutoCommitQuery() {return true;}
	
	@Override
    public void handleSelectHints(StringBuffer sql, SimpleFeatureType featureType, Query query) {
		LOGGER.fine("handleSelectHints()");
		LOGGER.fine("sql:\n" + sql);
		LOGGER.fine("Feature Type: " + featureType.getTypeName());
		LOGGER.fine("Query: " + query.toString());
		
		super.handleSelectHints(sql, featureType, query);
	}
	
	@Override
	public void handleUserDefinedType(ResultSet columns,
            ColumnMetadata metadata,
            Connection cx)
     throws SQLException {
		//handle our geometry type here
		
		LOGGER.fine("****DISTINCT type received, in handleUserDefinedType():");
        LOGGER.fine("column name: " + columns.getString("COLUMN_NAME"));
        LOGGER.fine("column type: " + columns.getString("TYPE_NAME"));
        LOGGER.fine("column dataType: " + columns.getInt("DATA_TYPE"));
        LOGGER.fine("column nullable: " + columns.getString("IS_NULLABLE"));

		ColumnMetadata column = new ColumnMetadata();
        column.setName(columns.getString("COLUMN_NAME"));
        column.setTypeName(columns.getString("TYPE_NAME"));
        column.setSqlType(columns.getInt("DATA_TYPE"));
        column.setNullable("YES".equalsIgnoreCase(columns.getString("IS_NULLABLE")));
        column.setBinding(getMapping(columns, cx));
//        column.setRestriction(getRestrictions(columns, cx));
	}

	@Override
	public void setGeometryValue(Geometry g, int dimension, int srid, Class binding, PreparedStatement ps, int column)
			throws SQLException {
	        if (g != null) {
	            ps.setString(column, new WKTWriter(dimension).write(g));
	            // ps.setString( column, g.toText() );
	        } else {
	            ps.setNull(column, Types.OTHER);
	        }
	}
	
	@Override
    public void prepareGeometryValue(
            Class<? extends Geometry> gClass,
            int dimension,
            int srid,
            Class binding,
            StringBuffer sql) {
		
		LOGGER.fine("prepareGeometryValue(): entered. ");
        if (gClass != null) {
        		LOGGER.fine("gClass = " +  gClass.getName());
                sql.append("st_geomFromWKT(?)");
            } 
        else {
        		LOGGER.fine("gClass = <null>");
                sql.append("st_geomFromWKT(?)");
            }
    }
	
	@Override
	public PreparedFilterToSQL createPreparedFilterToSQL() {
		LOGGER.fine("in createPreparedFilterToSQL()!");
		
		SedonaFilterToSQL f2s = new SedonaFilterToSQL(this);
        f2s.setCapabilities(f2s.createFilterCapabilities());
        return f2s;
    }
	
}
