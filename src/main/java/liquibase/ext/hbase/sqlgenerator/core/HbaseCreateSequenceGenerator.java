package liquibase.ext.hbase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.CreateSequenceGenerator;
import liquibase.statement.core.CreateSequenceStatement;
import liquibase.structure.core.Sequence;

public class HbaseCreateSequenceGenerator extends CreateSequenceGenerator {

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(CreateSequenceStatement statement, Database database) {
      return database.supportsSequences();
  }

  @Override
  public ValidationErrors validate(CreateSequenceStatement statement, Database database, 
      SqlGeneratorChain sqlGeneratorChain) {
    ValidationErrors validationErrors = new ValidationErrors();
    validationErrors.checkRequiredField("sequenceName", statement.getSequenceName());
    return validationErrors;
  }

  @Override
  public Sql[] generateSql(CreateSequenceStatement statement, Database database, 
      SqlGeneratorChain sqlGeneratorChain) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CREATE SEQUENCE ");
    buffer.append(database.escapeSequenceName(statement.getCatalogName(), 
        statement.getSchemaName(), statement.getSequenceName()));
    
    if (statement.getCacheSize() != null) {
      buffer.append(" CACHE ").append(statement.getCacheSize());
    }
    return new Sql[]{new UnparsedSql(buffer.toString(), getAffectedSequence(statement))};
  }
  
  protected Sequence getAffectedSequence(CreateSequenceStatement statement) {
    return new Sequence().setName(statement.getSequenceName()).
        setSchema(statement.getCatalogName(), statement.getSchemaName());
  }  
}
