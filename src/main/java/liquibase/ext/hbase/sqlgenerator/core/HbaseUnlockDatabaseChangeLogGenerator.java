package liquibase.ext.hbase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.hbase.statement.core.UpsertStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.UnlockDatabaseChangeLogGenerator;
import liquibase.statement.core.UnlockDatabaseChangeLogStatement;

public class HbaseUnlockDatabaseChangeLogGenerator extends UnlockDatabaseChangeLogGenerator {

    @Override
    public ValidationErrors validate(UnlockDatabaseChangeLogStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }
    
    @Override
    public int getPriority() {
      return PRIORITY_DATABASE;
    }


    @Override
    public Sql[] generateSql(UnlockDatabaseChangeLogStatement statement, Database database, 
        SqlGeneratorChain sqlGeneratorChain) {
      String liquibaseSchema = database.getLiquibaseSchemaName();

      UpsertStatement releaseStatement = new UpsertStatement(database.getLiquibaseCatalogName(), 
          liquibaseSchema, database.getDatabaseChangeLogLockTableName());
      releaseStatement.addNewColumnValue("ID", 1);
      releaseStatement.addNewColumnValue("LOCKED", false);
      releaseStatement.addNewColumnValue("LOCKGRANTED", null);
      releaseStatement.addNewColumnValue("LOCKEDBY", null);
      return SqlGeneratorFactory.getInstance().generateSql(releaseStatement, database);
    }
}
