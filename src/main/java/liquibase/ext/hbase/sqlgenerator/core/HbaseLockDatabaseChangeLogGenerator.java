package liquibase.ext.hbase.sqlgenerator.core;

import java.sql.Timestamp;

import liquibase.database.Database;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.hbase.statement.core.UpsertStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.sqlgenerator.core.LockDatabaseChangeLogGenerator;
import liquibase.statement.core.LockDatabaseChangeLogStatement;
import liquibase.util.NetUtil;

public class HbaseLockDatabaseChangeLogGenerator extends 
  LockDatabaseChangeLogGenerator {
  protected static final String hostname;
  protected static final String hostaddress;
  protected static final String hostDescription = 
      System.getProperty( "liquibase.hostDescription" ) == null ? "" : "#" + 
          System.getProperty( "liquibase.hostDescription" );
  
  static {
    try {
      hostname = NetUtil.getLocalHostName();
      hostaddress = NetUtil.getLocalHostAddress();
    } catch (Exception e) {
      throw new UnexpectedLiquibaseException(e);
    }
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public Sql[] generateSql(LockDatabaseChangeLogStatement statement, Database database, 
      SqlGeneratorChain sqlGeneratorChain) {
    String liquibaseSchema = database.getLiquibaseSchemaName();
    String liquibaseCatalog = database.getLiquibaseCatalogName();
  
    UpsertStatement updateStatement = new UpsertStatement(liquibaseCatalog, 
        liquibaseSchema, database.getDatabaseChangeLogLockTableName());
    updateStatement.addNewColumnValue("ID", 1);
    updateStatement.addNewColumnValue("LOCKED", Boolean.TRUE);
    updateStatement.addNewColumnValue("LOCKGRANTED", new Timestamp(new java.util.Date().getTime()));
    updateStatement.addNewColumnValue("LOCKEDBY", hostname + hostDescription + " (" + hostaddress + ")");
    return SqlGeneratorFactory.getInstance().generateSql(updateStatement, database);
  }
}