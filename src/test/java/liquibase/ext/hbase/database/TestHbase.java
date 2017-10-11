package liquibase.ext.hbase.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.junit.After;
import org.junit.Before;
public class TestHbase {

  Connection conn;
  
  @Before
  public void setup() {
    PhoenixDriver driver = new org.apache.phoenix.jdbc.PhoenixDriver();
    try {
      DriverManager.registerDriver(driver);
      String url = "jdbc:phoenix:ip-10-1-26-228.ec2.internal;schema=TEST_HBASE:2181:/hbase";
      conn = DriverManager.getConnection(url);
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @After
  public void tearDown() {
    /*
    try {
      //Drop tables/sequences created using liquibase core
      conn.prepareStatement("drop table TEST_HBASE.DATABASECHANGELOG").executeUpdate();
      conn.prepareStatement("drop table TEST_HBASE.DATABASECHANGELOGLOCK").executeUpdate();
      conn.prepareStatement("drop sequence TEST_HBASE.DATABASECHANGELOG_SEQ").executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }*/
  }
}
