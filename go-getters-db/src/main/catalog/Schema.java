package main.catalog;

import java.util.ArrayList;
import java.util.List;

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
     * @param col_idx index of requested column
     * @return requested column
     */
    public Column getColumn(int colIndx) {
        return columns.get(colIndx);
    }

    /**
     * @param col_name name of the wanted column
     * @return the column with the given name
     */

    /**
     * Looks up and returns the index of the first column in the schema with the specified name.
     * If multiple columns have the same name, the first such index is returned.
     * @param col_name name of column to look for
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

}