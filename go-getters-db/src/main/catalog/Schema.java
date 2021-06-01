package main.catalog;

import main.type.TypeID;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.COLUMN_SEP;

public class Schema {

    /** All the columns in the schema, inlined and uninlined. */
    private List<Column> columns;

    /**
     * Constructs the schema corresponding to the vector of columns, read left-to-right.
     * @param columns columns that describe the schema's individual columns
     */
    public Schema(List<Column> columns) {
        this.columns = new ArrayList<>(columns);
    }

    public static Schema copySchema( Schema from) {
        return new Schema(from.columns);
    }

    /** @return all the columns in the schema */
    public List<Column> getColumns() {
        return columns;
    }

    /**
     * Returns a specific column from the schema.
     * @param colIndx index of requested column
     * @return requested column
     */
    public Column getColumn(int colIndx) {
        return columns.get(colIndx);
    }


    /**
     * Looks up and returns the index of the first column in the schema with the specified name.
     * If multiple columns have the same name, the first such index is returned.
     * @param colName name of column to look for
     * @return the index of a column with the given name, throws an exception if it does not exist
     */
    public int getColIdx(String colName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName() == colName) {
                return i;
            }
        }
        return -1;
    }

    /** @return the number of columns in the schema for the tuple */
    public int getColumnCount() {
        return columns.size();
    }

    public List<TypeID> getColumnTypes() {
        return columns.stream().map(c-> c.getType()).toList();
    }
}