package liquibase.ext.hbase.database.jvm;

import java.sql.Connection;
import java.sql.SQLException;

import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;

public class HbaseJdbcConnection extends JdbcConnection {

  public HbaseJdbcConnection(Connection connection) {
    super(connection);
  }

  @Override
  public String getDatabaseProductName() throws DatabaseException {
    return "Hbase";
  }

}
