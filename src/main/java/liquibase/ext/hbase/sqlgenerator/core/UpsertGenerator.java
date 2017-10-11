package liquibase.ext.hbase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.structure.core.Relation;
import liquibase.structure.core.Table;

import liquibase.ext.hbase.statement.core.UpsertStatement;

import java.util.Date;

public class UpsertGenerator extends AbstractSqlGenerator<UpsertStatement> {

  @Override
  public ValidationErrors validate(UpsertStatement upsertStatement, Database database, 
      SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.checkRequiredField("tableName", upsertStatement.getTableName());
    validationErrors.checkRequiredField("columns", upsertStatement.getColumnValues());

    return validationErrors;
  }
  
  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(UpsertStatement statement, Database database, 
      SqlGeneratorChain sqlGeneratorChain) {
    
    StringBuffer sql = new StringBuffer("UPSERT INTO " + database.escapeTableName(
        statement.getCatalogName(), statement.getSchemaName(), statement.getTableName()) + " (");
    for (String column : statement.getNewColumnValues().keySet()) {
        sql.append(database.escapeColumnName(statement.getCatalogName(), 
            statement.getSchemaName(), statement.getTableName(), column)).append(", ");
    }
    sql.deleteCharAt(sql.lastIndexOf(" "));
    int lastComma = sql.lastIndexOf(",");
    if (lastComma >= 0) {
      sql.deleteCharAt(lastComma);
    }

    sql.append(") VALUES (");

    for (String column : statement.getNewColumnValues().keySet()) {
      Object newValue = statement.getNewColumnValues().get(column);
      if (newValue == null || newValue.toString().equalsIgnoreCase("NULL")) {
          sql.append("NULL");
      } else if (newValue instanceof String && 
          !looksLikeFunctionCall(((String) newValue), database)) {
          sql.append(DataTypeFactory.getInstance().fromObject(newValue, database).
              objectToSql(newValue, database));
      } else if (newValue instanceof Date) {
          sql.append(database.getDateLiteral(((Date) newValue)));
      } else if (newValue instanceof Boolean) {
          if (((Boolean) newValue)) {
              sql.append(DataTypeFactory.getInstance().getTrueBooleanValue(database));
          } else {
              sql.append(DataTypeFactory.getInstance().getFalseBooleanValue(database));
          }
      } else if (newValue instanceof DatabaseFunction) {
          sql.append(database.generateDatabaseFunctionValue((DatabaseFunction) newValue));
      }
      else {
          sql.append(newValue);
      }
      sql.append(", ");
    }

    sql.deleteCharAt(sql.lastIndexOf(" "));
    lastComma = sql.lastIndexOf(",");
    if (lastComma >= 0) {
        sql.deleteCharAt(lastComma);
    }

    sql.append(")");

    return new Sql[] {
            new UnparsedSql(sql.toString(), getAffectedTable(statement))
    };
  }
  
  protected Relation getAffectedTable(UpsertStatement statement) {
    return new Table().setName(statement.getTableName()).setSchema(statement.getCatalogName(), 
        statement.getSchemaName());
  }
}

