package main.catalog;

import main.execution.expressions.AbstractExpression;
import main.type.TypeID;

import java.util.Objects;

public class Column {

    private String columnName;

    private TypeID columnType;

    private AbstractExpression expr;

    public Column(String columnName, TypeID type) {
        this(columnName, type, null);
    }

    public Column(String columnName, TypeID type, AbstractExpression expr) {
        this.columnName = columnName;
        this.columnType = type;
        this.expr = expr;
    }

    public Column(String columnName, TypeID type, int length) {
        this(columnName, type, length, null);
    }

    public Column(String columnName, TypeID type, int length, AbstractExpression expr) {
        this.columnName = columnName;
        this.columnType = type;
        this.expr = expr;
    }

    public String getName() {
        return columnName;
    }

    public TypeID getType() {
        return columnType;
    }

    public AbstractExpression getExpr() {
        return expr;
    }


}
