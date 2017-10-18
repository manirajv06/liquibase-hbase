package liquibase.ext.hbase.diff.compare.core;

import liquibase.database.Database;
import liquibase.diff.compare.CompareControl;
import liquibase.diff.compare.DatabaseObjectComparatorChain;
import liquibase.diff.compare.core.SchemaComparator;
import liquibase.ext.hbase.database.HbaseDatabase;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.util.StringUtils;


public class HbaseSchemaComparator extends SchemaComparator {
  
    
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
        if (Schema.class.isAssignableFrom(objectType)) {
            return PRIORITY_DATABASE;
        }
        return PRIORITY_NONE;
    }

    

    @Override
    public boolean isSameObject(DatabaseObject databaseObject1, DatabaseObject databaseObject2, Database accordingTo, DatabaseObjectComparatorChain chain) {
        if (!(databaseObject1 instanceof Schema && databaseObject2 instanceof Schema)) {
            return false;
        }

        String schemaName1 = null;
        String schemaName2 = null;

        if (accordingTo.supportsSchemas()) {
            schemaName1 = databaseObject1.getName();
            schemaName2 = databaseObject2.getName();

        } else if (accordingTo.supportsCatalogs()) {
            schemaName1 = ((Schema) databaseObject1).getCatalogName();
            schemaName2 = ((Schema) databaseObject2).getCatalogName();
        }


        if (StringUtils.trimToEmpty(schemaName1).equalsIgnoreCase(StringUtils.trimToEmpty(schemaName2))) {
            return true;
        }

        //switch off default names and then compare again
        if (schemaName1 == null) {
            if (accordingTo.supportsSchemas()) {
                schemaName1 = accordingTo.getDefaultSchemaName();
            } else if (accordingTo.supportsCatalogs()) {
                schemaName1 = accordingTo.getDefaultCatalogName();
            }
        }
        
        if(schemaName1.equalsIgnoreCase(((HbaseDatabase) accordingTo).getNativeDefaultSchema())) {
          if (accordingTo.supportsSchemas()) {
            schemaName1 = "";
          } else if (accordingTo.supportsCatalogs()) {
            schemaName1 = accordingTo.getDefaultCatalogName();
          }
        }
        
        if (schemaName2 == null) {
            if (accordingTo.supportsSchemas()) {
                schemaName2 = accordingTo.getDefaultSchemaName();
            } else if (accordingTo.supportsCatalogs()) {
                schemaName2 = accordingTo.getDefaultCatalogName();
            }
        }
        
        if(schemaName2.equalsIgnoreCase(((HbaseDatabase) accordingTo).getNativeDefaultSchema())) {
          if (accordingTo.supportsSchemas()) {
            schemaName2 = "";
          } else if (accordingTo.supportsCatalogs()) {
            schemaName2 = accordingTo.getDefaultCatalogName();
          }
        }

        
        if (StringUtils.trimToEmpty(schemaName1).equalsIgnoreCase(StringUtils.trimToEmpty(schemaName2))) {
            return true;
        }

        //check with schemaComparisons
        if (chain.getSchemaComparisons() != null && chain.getSchemaComparisons().length > 0) {
            for (CompareControl.SchemaComparison comparison : chain.getSchemaComparisons()) {
                String comparisonSchema1;
                String comparisonSchema2;
                if (accordingTo.supportsSchemas()) {
                    comparisonSchema1 = comparison.getComparisonSchema().getSchemaName();
                    comparisonSchema2 = comparison.getReferenceSchema().getSchemaName();
                } else if (accordingTo.supportsCatalogs()) {
                    comparisonSchema1 = comparison.getComparisonSchema().getCatalogName();
                    comparisonSchema2 = comparison.getReferenceSchema().getCatalogName();
                } else {
                    break;
                }

                String finalSchema1 = schemaName1;
                String finalSchema2 = schemaName2;

                if (comparisonSchema1 != null && comparisonSchema1.equalsIgnoreCase(schemaName1)) {
                    finalSchema1 = comparisonSchema2;
                } else if (comparisonSchema2 != null && comparisonSchema2.equalsIgnoreCase(schemaName1)) {
                    finalSchema1 = comparisonSchema1;
                }

                if (StringUtils.trimToEmpty(finalSchema1).equalsIgnoreCase(StringUtils.trimToEmpty(finalSchema2))) {
                    return true;
                }

                if (comparisonSchema1 != null && comparisonSchema1.equalsIgnoreCase(schemaName2)) {
                    finalSchema2 = comparisonSchema2;
                } else if (comparisonSchema2 != null && comparisonSchema2.equalsIgnoreCase(schemaName2)) {
                    finalSchema2 = comparisonSchema1;
                }

                if (StringUtils.trimToEmpty(finalSchema1).equalsIgnoreCase(StringUtils.trimToEmpty(finalSchema2))) {
                    return true;
                }
            }
        }

        schemaName1 = ((Schema) databaseObject1).toCatalogAndSchema().getSchemaName();
        schemaName1 = ((Schema) databaseObject1).toCatalogAndSchema().getSchemaName();
        
        return StringUtils.trimToEmpty(schemaName1).equalsIgnoreCase(StringUtils.trimToEmpty(schemaName2));
    }
}
