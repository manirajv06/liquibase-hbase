package liquibase.ext.hbase.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
public class TestHbase {

  static Connection conn;
  static Connection connForDefaultSchema;
  
  @BeforeClass
  public static void setup() {
    PhoenixDriver driver = new org.apache.phoenix.jdbc.PhoenixDriver();
    try {
      DriverManager.registerDriver(driver);
      String url = "jdbc:phoenix:ip-10-1-26-228.ec2.internal:2181:/hbase";
      connForDefaultSchema = DriverManager.getConnection(url);
      String url2 = "jdbc:phoenix:ip-10-1-26-228.ec2.internal;schema_name=TEST_HBASE:2181:/hbase";
      conn = DriverManager.getConnection(url2);
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  @AfterClass
  public static void tearDown() {
    try {
      //Drop tables/sequences created using liquibase core
      conn.prepareStatement("drop table if exists TEST_HBASE.DATABASECHANGELOG").executeUpdate();
      conn.prepareStatement("drop table if exists TEST_HBASE.DATABASECHANGELOGLOCK").executeUpdate();
      conn.prepareStatement("drop sequence if exists TEST_HBASE.DATABASECHANGELOG_SEQ").executeUpdate();
      conn.prepareStatement("drop table if exists \"\".DATABASECHANGELOG").executeUpdate();
      conn.prepareStatement("drop table if exists \"\".DATABASECHANGELOGLOCK").executeUpdate();
      conn.prepareStatement("drop sequence if exists \"\".DATABASECHANGELOG_SEQ").executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
