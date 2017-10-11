package liquibase.ext.hbase.sqlgenerator.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import liquibase.database.Database;
import liquibase.ext.hbase.statement.core.UpsertStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.InitializeDatabaseChangeLogLockTableGenerator;
import liquibase.statement.core.InitializeDatabaseChangeLogLockTableStatement;
import liquibase.statement.core.InsertStatement;

public class HbaseInitializeDatabaseChangeLogLockTableGenerator extends 
  InitializeDatabaseChangeLogLockTableGenerator {

  @Override
  public int getPriority() {
      return PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(InitializeDatabaseChangeLogLockTableStatement statement, 
      Database database, SqlGeneratorChain sqlGeneratorChain) {
    
    UpsertStatement upsertStatement = new UpsertStatement(database.getLiquibaseCatalogName(), 
        database.getLiquibaseSchemaName(), database.getDatabaseChangeLogLockTableName());
    upsertStatement.addNewColumnValue("ID", 1);
    upsertStatement.addNewColumnValue("LOCKED", Boolean.FALSE);

    List<Sql> sql = new ArrayList<Sql>();
    sql.addAll(Arrays.asList(SqlGeneratorFactory.getInstance().generateSql(upsertStatement, database)));
    return sql.toArray(new Sql[sql.size()]);
  }
}
