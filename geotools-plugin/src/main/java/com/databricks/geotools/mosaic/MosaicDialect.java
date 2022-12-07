package com.databricks.geotools.mosaic;

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
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.GeometryDescriptor;

import com.databricks.geotools.DatabricksSQLDialect;
import com.databricks.geotools.LayersList;


/**
 * MosaicDialect for Databricks Lakehouse.
 *
 * @author Tom Diepenbrock <tom.diepenbrock@databricks.com>
 */
public class MosaicDialect extends DatabricksSQLDialect {


	private Logger LOGGER;
	
    public MosaicDialect(JDBCDataStore dataStore, Integer refreshPeriod) {
        super(dataStore, refreshPeriod);
    	this.LOGGER = org.geotools.util.logging.Logging.getLogger(getClass());
    }

    
    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
    	LOGGER.fine("in registerSqlTypeNameToClassMappings()");
        super.registerSqlTypeNameToClassMappings(mappings);
        
        mappings.put("BINARY", Geometry.class);
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

        if (typeName.equals("BINARY") && dataType == -2) {
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
    	
    	ArrayList<ReferencedEnvelope> list = new ArrayList<ReferencedEnvelope>();

        double minX = -180.0;
        double minY = -90.0;
        double maxX = 180.0;
        double maxY = 90.0;
        
        list.add(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84));
        
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
        byte[] value = rs.getBytes(column);
//        LOGGER.fine("value: " + value);
        
        try {
        	return wkbReader.get().read(value);
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
                sql.append("st_geomfromwkt(?)");
            } 
        else {
        		LOGGER.fine("gClass = <null>");
                sql.append("st_geomfromwkt(?)");
            }
    }
	
	@Override
	public PreparedFilterToSQL createPreparedFilterToSQL() {
		LOGGER.fine("in createPreparedFilterToSQL()!");
		
		MosaicFilterToSQL f2s = new MosaicFilterToSQL(this);
        f2s.setCapabilities(f2s.createFilterCapabilities());
        return f2s;
    }
	
}
