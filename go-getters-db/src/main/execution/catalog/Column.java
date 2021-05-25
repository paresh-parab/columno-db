package main.execution.catalog;

public class Column {
    String columnName;
    String typeId;
    Integer length;

    public Column(String columnName, String typeId, Integer length) {
        this.columnName = columnName;
        this.typeId = typeId;
        this.length = length;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTypeId() {
        return typeId;
    }

    public Integer getLength() {
        return length;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void setTypeId(String typeId) {
        this.typeId = typeId;
    }

    public void setLength(Integer length) {
        this.length = length;
    }
}
