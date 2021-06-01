package main.catalog;

import main.execution.expressions.AbstractExpression;
import main.type.TypeID;

import java.util.Objects;

public class Column {

    private String columnName;
    /**
     * Column value's type.
     */
    private TypeID columnType;
    /**
     * Expression used to create this column
     **/
    private AbstractExpression expr;
    /**
     * Non-variable-length constructor for creating a Column.
     *
     * @param column_name name of the column
     * @param type        type of the column
     * @param expr        expression used to create this column
     */
    public Column(String columnName, TypeID type) {
        this(columnName, type, null);
    }

    public Column(String columnName, TypeID type, AbstractExpression expr) {
        this.columnName = columnName;
        this.columnType = type;
        this.expr = expr;
    }

    /**
     * Variable-length constructor for creating a Column.
     *
     * @param column_name name of the column
     * @param type        type of column
     * @param length      length of the varlen
     * @param expr        expression used to create this column
     */
    public Column(String columnName, TypeID type, int length) {
        this(columnName, type, length, null);
    }

    public Column(String columnName, TypeID type, int length, AbstractExpression expr) {
        this.columnName = columnName;
        this.columnType = type;
        this.expr = expr;
    }

    /**
     * @return column name
     */
    public String getName() {
        return columnName;
    }

    /**
     * @return column type
     */
    public TypeID getType() {
        return columnType;
    }

    public AbstractExpression getExpr() {
        return expr;
    }


}
