package liquibase.ext.hbase.statement.core;

import java.math.BigInteger;

import liquibase.statement.core.CreateSequenceStatement;

public class HbaseCreateSequenceStatement extends CreateSequenceStatement {
  
  private BigInteger cacheSize = BigInteger.ONE;
  
  public HbaseCreateSequenceStatement(String catalogName, String schemaName,
      String sequenceName) {
    super(catalogName, schemaName, sequenceName);
    super.setCacheSize(cacheSize);
  }
}
