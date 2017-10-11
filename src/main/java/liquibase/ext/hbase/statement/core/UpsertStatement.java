package liquibase.ext.hbase.statement.core;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import liquibase.change.ColumnConfig;
import liquibase.statement.AbstractSqlStatement;

public class UpsertStatement extends AbstractSqlStatement {
  private String catalogName;
  private String schemaName;
  private String tableName;
  private Map<String, Object> columnValues = new LinkedHashMap<String, Object>();
  private SortedMap<String, Object> newColumnValues = new TreeMap<String, Object>();

  public UpsertStatement(String catalogName, String schemaName, 
      String tableName) {
    this.catalogName = catalogName;
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public UpsertStatement addColumnValue(String columnName, Object newValue) {
    columnValues.put(columnName, newValue);
    return this;
  }

  public Object getColumnValue(String columnName) {
    return columnValues.get(columnName);
  }

  public Map<String, Object> getColumnValues() {
    return columnValues;
  }
  
  public UpsertStatement addColumn(ColumnConfig columnConfig) {
    return addColumnValue(columnConfig.getName(), columnConfig.getValueObject());
  }
  
  public UpsertStatement addNewColumnValue(String columnName, Object newValue) {
    newColumnValues.put(columnName, newValue);
    return this;
  }

  public Map<String, Object> getNewColumnValues() {
    return newColumnValues;
  }

}