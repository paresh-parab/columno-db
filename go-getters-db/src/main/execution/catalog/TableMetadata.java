package main.execution.catalog;

public class TableMetadata {
    Schema schema;
    String tableName;
    Integer tableId;

    public TableMetadata(Schema schema, String tableName, Integer tableId) {
        this.schema = schema;
        this.tableName = tableName;
        this.tableId = tableId;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getTableName() {
        return tableName;
    }

    public Integer getTableId() {
        return tableId;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }
}
