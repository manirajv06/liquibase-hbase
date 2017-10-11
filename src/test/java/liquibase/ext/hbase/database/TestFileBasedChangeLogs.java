package liquibase.ext.hbase.database;

import org.junit.Test;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.exception.LiquibaseException;
import liquibase.ext.hbase.database.jvm.HbaseJdbcConnection;
import liquibase.resource.FileSystemResourceAccessor;

public class TestFileBasedChangeLogs extends TestHbase {

  @Test
  public  void testFileBasedChangeLogs() throws LiquibaseException {
    DatabaseFactory.getInstance().register(new HbaseDatabase());
    Database database = DatabaseFactory.getInstance().
        findCorrectDatabaseImplementation(new HbaseJdbcConnection(conn));
    
    Liquibase liquibase = 
        new liquibase.Liquibase("./src/test/resources/changelog.xml", 
        new FileSystemResourceAccessor(), database);
  
    liquibase.update(new Contexts(), new LabelExpression());
  }
}
