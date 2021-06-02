package main.catalog;

import main.type.TypeID;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static main.common.Constants.COLUMN_SEP;

public class Schema {

    private List<Column> columns;

    public Schema(List<Column> columns) {
        this.columns = new ArrayList<>(columns);
    }

    public static Schema copySchema( Schema from) {
        return new Schema(from.columns);
    }

    public List<Column> getColumns() {
        return columns;
    }

    public Column getColumn(int colIndx) {
        return columns.get(colIndx);
    }

    public int getColIdx(String colName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName() == colName) {
                return i;
            }
        }
        return -1;
    }

    public int getColumnCount() {
        return columns.size();
    }

    public List<TypeID> getColumnTypes() {
        return columns.stream().map(c-> c.getType()).collect(Collectors.toList());
    }
}