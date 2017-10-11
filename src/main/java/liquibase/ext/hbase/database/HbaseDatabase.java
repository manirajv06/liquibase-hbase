/**
 * Copyright 2010 Open Pricer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package liquibase.ext.hbase.database;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.ObjectQuotingStrategy;
import liquibase.exception.DatabaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SequenceCurrentValueFunction;
import liquibase.statement.SequenceNextValueFunction;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Sequence;

/**
 * HBase implementation for liquibase
 *
 */
public class HbaseDatabase extends AbstractJdbcDatabase {

  private static final Logger LOG = LogFactory.getInstance().getLog();
  private final String databaseProductName;
  private final String prefix;
  private final String databaseDriver;
  private final int PORT_NO = 2181;

  public HbaseDatabase() {
    this("Hbase", "jdbc:phoenix", "org.apache.phoenix.jdbc.PhoenixDriver");
  }
  
	public HbaseDatabase(String databaseProductName, String prefix, 
	      String databaseDriver) {
	  this.databaseProductName = databaseProductName;
	  this.prefix = prefix;
	  this.databaseDriver = databaseDriver;
	  super.sequenceNextValueFunction = "NEXT VALUE FOR %S"; 
	  super.sequenceCurrentValueFunction = "CURRENT VALUE FOR  %s";
	  
    this.addReservedWords(Arrays.asList("SELECT", "FROM", "WHERE", "NOT", "AND", "OR", 
        "NULL", "TRUE", "FALSE", "LIKE", "ILIKE", "AS", "OUTER", "ON", "OFF", "IN", 
        "GROUP", "HAVING", "ORDER", "BY", "ASC", "DESC", "NULLS", "LIMIT", "FIRST", 
        "LAST", "CASE", "WHEN", "THEN", "ELSE", "END", "EXISTS", "IS", "FIRST", 
        "DISTINCT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "BETWEEN", "UPSERT", 
        "INTO", "VALUES", "DELETE", "CREATE", "DROP", "PRIMARY", "KEY", "ALTER", 
        "COLUMN", "SESSION", "TABLE", "SCHEMA", "ADD", "SPLIT", "EXPLAIN", "VIEW", 
        "IF", "CONSTRAINT", "TABLES", "ALL", "INDEX", "INCLUDE", "WITHIN", "SET", 
        "CAST", "ACTIVE", "USABLE", "UNUSABLE", "DISABLE", "REBUILD", "ARRAY", 
        "SEQUENCE", "START", "WITH", "INCREMENT", "NEXT", "CURRENT", "VALUE", 
        "FOR", "CACHE", "LOCAL", "ANY", "SOME", "MINVALUE", "MAXVALUE", "CYCLE", 
        "CASCADE", "UPDATE", "STATISTICS", "COLUMNS", "TRACE", "ASYNC", "SAMPLING", 
        "TABLESAMPLE", "UNION", "FUNCTION", "AS", "TEMPORARY", "RETURNS", "USING", 
        "JAR", "DEFAULTVALUE", "CONSTANT", "REPLACE", "LIST", "JARS", "ROW_TIMESTAMP", 
        "USE", "OFFSET", "FETCH", "DECLARE", "CURSOR", "OPEN", "CLOSE", "ROW", "ROWS", 
        "ONLY", "EXECUTE", "UPGRADE", "DEFAULT", "DUPLICATE", "IGNORE", "IMMUTABLE"));
	}

	@Override
	public boolean isCorrectDatabaseImplementation(DatabaseConnection conn)
	    throws DatabaseException {
		return databaseProductName.equals("Hbase");
	}
	

	@Override
	public String getDefaultDriver(String url) {
	  if (url.startsWith(prefix)) {
	    return databaseDriver;
	  }
	  return null;
	}

	@Override
  public String getShortName() {
	  return databaseProductName.toLowerCase();
  }

  @Override
  protected String getDefaultDatabaseProductName() {
    return databaseProductName;
  }

  @Override
  public Integer getDefaultPort() {
    return PORT_NO;
  }

  @Override
  public String getCurrentDateTimeFunction() {
    return "current_time()";
  }

  @Override
  protected String getConnectionSchemaName() {
    boolean flag = false;
    String schemaName = null;
    try {
      String tokens[] = super.getConnection().getURL().split(";");
      if(tokens.length > 1) {
        String schemaInfo[] = tokens[1].split(":");
        if(schemaInfo.length > 1) {
          String[] schemaDetails = schemaInfo[0].split("=");
          if(schemaDetails.length > 1) {
            schemaName = schemaDetails[1].toUpperCase();
            flag = true;
          }
        }
      }
    } catch (Exception e) {
      LOG.debug("Unable to extract the schema name\t[ex: " + e.getMessage() + "]");
    }
    
    if(!flag) {
      LOG.warning("Unable to extract the schema from url\t[url: " + super.getConnection().getURL() + "]");
    }
    return schemaName;
  }
  
  @Override
	public boolean supportsInitiallyDeferrableColumns() {
		return true;
	}

	@Override
	public boolean supportsTablespaces() {
		return false;
	}

	@Override
	public int getPriority() {
		return PRIORITY_DATABASE;
	}

	@Override
	public boolean supportsDDLInTransaction() {
		return false;
	}

	@Override
  public boolean supportsSchemas() {
    return true;
  }
	
  @Override
  public boolean supportsCatalogs() {
      return false;
  }


  @Override
  public String escapeObjectName(String objectName, Class<? extends DatabaseObject> objectType) {
      if (quotingStrategy == ObjectQuotingStrategy.QUOTE_ALL_OBJECTS) {
          return super.escapeObjectName(objectName, objectType);
      }
      if (objectName != null && 
          quotingStrategy != ObjectQuotingStrategy.QUOTE_ALL_OBJECTS && 
          isReservedWord(objectName.toUpperCase())) {
              return "\""+objectName.toUpperCase()+"\"";
      }
      return objectName;
  }

  /**
	 * Exact Copy of AbstractJdbcDatabase#isCurrentTimeFunction to 
	 * satisfy HbaseDatabase#generateDatabaseFunctionValue()
	 * 
	 * @param functionValue
	 * @return
	 */
  private boolean isCurrentTimeFunction(final String functionValue) {
    if (functionValue == null) {
        return false;
    }

    return functionValue.startsWith("current_timestamp")
            || functionValue.startsWith("current_datetime")
            || functionValue.equalsIgnoreCase(getCurrentDateTimeFunction());
  }
	
  /**
   * Copy of AbstractJdbcDatabase#generateDatabaseFunctionValue() with minor diff as
   * described below
   * 
   * Internal sequenceName has dot(.) at this moment for sure it prefixed with "<schema_name>."
   * To pass through, we are by passing escapeObjectName method which escapes using 
   * AbstractJdbcDatabase#quotingStartCharacter and AbstractJdbcDatabase#quotingEndCharacter
   * Hence, Safer to not allow dot(.) in external sequence names coming through changesets as 
   * those chars won't be escaped
   */
  @Override
  public String generateDatabaseFunctionValue(final DatabaseFunction databaseFunction) {
      if (databaseFunction.getValue() == null) {
          return null;
      }
      if (isCurrentTimeFunction(databaseFunction.getValue().toLowerCase())) {
          return getCurrentDateTimeFunction();
      } else if (databaseFunction instanceof SequenceNextValueFunction) {
          if (sequenceNextValueFunction == null) {
              throw new RuntimeException(String.format("next value function for a sequence is not configured for database %s",
                      getDefaultDatabaseProductName()));
          }
          String sequenceName = databaseFunction.getValue();
          if (!sequenceNextValueFunction.contains("'") && 
              !sequenceName.contains(".")) {
            sequenceName = escapeObjectName(sequenceName, Sequence.class);
          }
          return String.format(sequenceNextValueFunction, sequenceName);
      } else if (databaseFunction instanceof SequenceCurrentValueFunction) {
          if (sequenceCurrentValueFunction == null) {
              throw new RuntimeException(String.format("current value function for a sequence is not configured for database %s",
                      getDefaultDatabaseProductName()));
          }

          String sequenceName = databaseFunction.getValue();
          if (!sequenceCurrentValueFunction.contains("'") && 
              !sequenceName.contains(".")) {
              sequenceName = escapeObjectName(sequenceName, Sequence.class);
          }
          String s = String.format(sequenceCurrentValueFunction, sequenceName);
          return s;
      } else {
          return databaseFunction.getValue();
      }
  }

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getDateTimeLiteral(Timestamp date) {
		return "'"+new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(date)+"'";
	}

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getDateLiteral(Date date) {
		return "'"+new SimpleDateFormat("yyyy-MM-dd").format(date)+"'";
	}

	/**
	 * Use JDBC escape syntax
	 */
	@Override
	public String getTimeLiteral(Time date) {
		return "'"+new SimpleDateFormat("hh:mm:ss.SSS").format(date)+"'";
	}
}
