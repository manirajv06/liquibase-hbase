package liquibase.ext.hbase.datatype;


import liquibase.database.Database;
import liquibase.datatype.DataTypeInfo;
import liquibase.datatype.DatabaseDataType;
import liquibase.datatype.LiquibaseDataType;
import liquibase.datatype.core.IntType;
import liquibase.ext.hbase.database.HbaseDatabase;

@DataTypeInfo(name = "int", aliases = { 
    "integer", "java.sql.Types.INTEGER", 
    "java.lang.Integer", 
    "serial", 
    "int4", 
    "serial4" }, 
minParameters = 0, maxParameters = 1, priority = LiquibaseDataType.PRIORITY_DATABASE)
public class HbaseIntType extends IntType {
  @Override
  public DatabaseDataType toDatabaseDataType(Database database) {
      if (database instanceof HbaseDatabase) {
          return new DatabaseDataType("INTEGER");
      }
      return super.toDatabaseDataType(database);
  }

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

}
