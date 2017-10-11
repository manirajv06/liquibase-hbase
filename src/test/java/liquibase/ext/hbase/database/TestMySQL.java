package liquibase.ext.hbase.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestMySQL {

  
  public static void main(String[] s) {
 
    try {
      Statement stmt;
      ResultSet rs;
      Class.forName("com.mysql.jdbc.Driver");
      String url = "jdbc:mysql://10.1.26.228:3306/oozie?useSSL=false&characterEncoding=UTF-8";
      Connection con = (Connection) DriverManager.getConnection(url, "root", "Bee_1000");

         // con.setCharacterEncoding("utf-8");
          stmt = (Statement) con.createStatement();
          //stmt.executeQuery("SET NAMES 'UTF8'");
          //stmt.executeQuery("SET CHARACTER SET 'UTF8'");
          //String greekname = "κωνσταντίνα";
          //stmt.executeUpdate("INSERT INTO categories(category_id,category_name) VALUES ('" + 17 + "','" + greekname + "')");
          
          PreparedStatement pstmt =
              con.prepareStatement("INSERT INTO test1(error_message) VALUES (?)");
          
          
          String dd = "row '���VS                                   !' on table 'USER_SEGMENT_INSTANCE_MEMBERS' at region=USER_SEGMENT_INSTANCE_MEMBERS,\\x06\\x80\\x00\\x04\\xA2\\x80\\x01\"\\xEAbb3741d9-3557-38ae-8d4d-59c98701f38d,1506510742377.d864b7a540aa5e63c48612940dc21151.,";
          
          pstmt.setString(1, dd);
          pstmt.execute();
          //stmt.executeUpdate("INSERT INTO test(error_message) VALUES ('" + dd + "')");
          con.close();
      } catch (Exception e) {
          System.out.println("problem during the connection with the database!"+e);
      }
    
  }
}
