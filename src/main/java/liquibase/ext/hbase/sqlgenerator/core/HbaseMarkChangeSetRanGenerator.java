package liquibase.ext.hbase.sqlgenerator.core;

import liquibase.change.Change;
import liquibase.change.core.TagDatabaseChange;
import liquibase.changelog.ChangeLogHistoryServiceFactory;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hbase.statement.core.UpsertStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.MarkChangeSetRanGenerator;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.SequenceNextValueFunction;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.MarkChangeSetRanStatement;
import liquibase.util.LiquibaseUtil;
import liquibase.util.StringUtils;

public class HbaseMarkChangeSetRanGenerator extends 
  MarkChangeSetRanGenerator {

  @Override
  public ValidationErrors validate(MarkChangeSetRanStatement statement, 
      Database database, SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.checkRequiredField("changeSet", statement.getChangeSet());
    validationErrors.checkRequiredField("id", statement.getChangeSet().getId());
    validationErrors.checkRequiredField("author", statement.getChangeSet().getAuthor());
    validationErrors.checkRequiredField("filename", statement.getChangeSet().getFilePath());
    return validationErrors;
  }

  @Override
  public int getPriority() {
      return PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(MarkChangeSetRanStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
    ChangeSet changeSet = statement.getChangeSet();
  
    SqlStatement runStatement;
    String sequenceName = database.getDatabaseChangeLogTableName() + "_SEQ";
    
    try {
      if (statement.getExecType().equals(ChangeSet.ExecType.FAILED) || 
          statement.getExecType().equals(ChangeSet.ExecType.SKIPPED)) {
        return new Sql[0]; //don't mark
      } 
      
      String tag = null;
      for (Change change : changeSet.getChanges()) {
          if (change instanceof TagDatabaseChange) {
              TagDatabaseChange tagChange = (TagDatabaseChange) change;
              tag = tagChange.getTag();
          }
      }
      
      if (statement.getExecType().ranBefore) {
        
        //TODO: Is Sequence exists?
        
        runStatement = new UpsertStatement(database.getLiquibaseCatalogName(), 
            database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
            .addNewColumnValue("PLACEHOLDER_PK", new DatabaseFunction(
                database.generateDatabaseFunctionValue(
                    new SequenceNextValueFunction(database.escapeSequenceName(database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(), sequenceName)))))
            .addNewColumnValue("DATEEXECUTED", new DatabaseFunction(database.getCurrentDateTimeFunction()))
            .addNewColumnValue("MD5SUM", changeSet.generateCheckSum().toString())
            .addNewColumnValue("EXECTYPE", statement.getExecType().value)
            .addNewColumnValue("DEPLOYMENT_ID", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId());
        
        if (tag != null) {
          ((UpsertStatement) runStatement).addNewColumnValue("TAG", tag);
        }

      } else {
        runStatement = new UpsertStatement(database.getLiquibaseCatalogName(), 
            database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
            
            //TODO: Is Sequence exists?
            
            .addNewColumnValue("PLACEHOLDER_PK", new DatabaseFunction(
                database.generateDatabaseFunctionValue(
                    new SequenceNextValueFunction(database.escapeSequenceName(database.getLiquibaseCatalogName(),
                        database.getLiquibaseSchemaName(), sequenceName)))))
            .addNewColumnValue("ID", changeSet.getId())
            .addNewColumnValue("AUTHOR", changeSet.getAuthor())
            .addNewColumnValue("FILENAME", changeSet.getFilePath())
            .addNewColumnValue("DATEEXECUTED", new DatabaseFunction(database.getCurrentDateTimeFunction()))
            .addNewColumnValue("ORDEREXECUTED", ChangeLogHistoryServiceFactory.getInstance().
                getChangeLogService(database).getNextSequenceValue())
            .addNewColumnValue("MD5SUM", changeSet.generateCheckSum().toString())
            .addNewColumnValue("DESCRIPTION", limitSize(changeSet.getDescription()))
            .addNewColumnValue("COMMENTS", limitSize(StringUtils.trimToEmpty(changeSet.getComments())))
            .addNewColumnValue("EXECTYPE", statement.getExecType().value)
            .addNewColumnValue("LIQUIBASE", LiquibaseUtil.getBuildVersion().replaceAll("SNAPSHOT", "SNP"))
            .addNewColumnValue("DEPLOYMENT_ID", ChangeLogHistoryServiceFactory.getInstance().getChangeLogService(database).getDeploymentId());
  
          if (tag != null) {
              ((UpsertStatement) runStatement).addNewColumnValue("TAG", tag);
          }
      }
    } catch (LiquibaseException e) {
        throw new UnexpectedLiquibaseException(e);
    }
  
    return SqlGeneratorFactory.getInstance().generateSql(
        new SqlStatement[] {runStatement}, 
        database);
  }

  private String limitSize(String string) {
      int maxLength = 250;
      if (string.length() > maxLength) {
          return string.substring(0, maxLength - 3) + "...";
      }
      return string;
  }
}