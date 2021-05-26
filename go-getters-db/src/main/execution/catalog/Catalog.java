package main.execution.catalog;

import java.util.HashMap;
import java.util.Map;

public class Catalog {
        Map<String,Integer> namesToIdentifier = new HashMap<>();
        Map<Integer, TableMetadata> TableIdentifierToMetadata = new HashMap<>();
        Integer next_table_oid_= 0;
        public String createTable(Transaction txn, String tableName, Schema schema){
                Integer table_oid = next_table_oid_;
                next_table_oid_++;
                namesToIdentifier.put(tableName,table_oid);
                TableIdentifierToMetadata.put(table_oid,new TableMetadata(schema,tableName,table_oid));
                return TableIdentifierToMetadata.get(table_oid).getTableName();

        }

        public TableMetadata getTableMetadata(String tableName){
                Integer table_oid = namesToIdentifier.get(tableName);
                return TableIdentifierToMetadata.get(table_oid);
        }





}
