package liquibase.ext.hbase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.datatype.DataTypeFactory;
import liquibase.ext.hbase.database.HbaseDatabase;
import liquibase.ext.hbase.statement.core.HbaseCreateSequenceStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.NotNullConstraint;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.CreateDatabaseChangeLogTableStatement;
import liquibase.statement.core.CreateTableStatement;
import liquibase.sqlgenerator.core.CreateDatabaseChangeLogTableGenerator;

public class HbaseCreateDatabaseChangeLogTableGenerator extends CreateDatabaseChangeLogTableGenerator {

  @Override
  public boolean supports(CreateDatabaseChangeLogTableStatement statement, Database database) {
      return database instanceof HbaseDatabase && super.supports(statement, database);
  }

  @Override
  public int getPriority() {
      return PRIORITY_DATABASE;
  }
  
  @Override
  public Sql[] generateSql(CreateDatabaseChangeLogTableStatement statement, 
      Database database, SqlGeneratorChain sqlGeneratorChain) {
    
    //Add a dummy column "PLACEHOLDER_PK" to satisfy create table requirements
    CreateTableStatement createTableStatement = new CreateTableStatement(database.getLiquibaseCatalogName(), 
        database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName())
      .setTablespace(database.getLiquibaseTablespaceName())
      .addPrimaryKeyColumn("PLACEHOLDER_PK", 
          DataTypeFactory.getInstance().fromDescription("BIGINT", database), 
          null, null, null, new NotNullConstraint())
      .addColumn("ID", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database), null, null, null)
      .addColumn("AUTHOR", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database), null, null, null)
      .addColumn("FILENAME", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database), null, null, null)
      .addColumn("DATEEXECUTED", DataTypeFactory.getInstance().fromDescription(
          "DATE", database), null, null)
      .addColumn("ORDEREXECUTED", DataTypeFactory.getInstance().fromDescription(
          "BIGINT", database), null)
      .addColumn("EXECTYPE", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(10)", database), null)
      .addColumn("MD5SUM", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(35)", database))
      .addColumn("DESCRIPTION", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database))
      .addColumn("COMMENTS", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database))
      .addColumn("TAG", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(255)", database))
      .addColumn("LIQUIBASE", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(20)", database))
      .addColumn("LABELS", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR("+getLabelsSize()+")", database))
      .addColumn("DEPLOYMENT_ID", DataTypeFactory.getInstance().fromDescription(
          "VARCHAR(10)", database));

    SqlStatement sequenceStatement = new HbaseCreateSequenceStatement(database.getLiquibaseCatalogName(), 
        database.getLiquibaseSchemaName(), database.getDatabaseChangeLogTableName() + "_SEQ");

    return SqlGeneratorFactory.getInstance().generateSql(
        new SqlStatement[] {createTableStatement, sequenceStatement}, 
        database);
  
  }

  protected String getIdColumnSize() {
      return "255";
  }

  protected String getAuthorColumnSize() {
      return "255";
  }

  protected String getFilenameColumnSize() {
      return "255";
  }
}

