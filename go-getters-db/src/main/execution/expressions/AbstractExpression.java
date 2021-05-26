package main.execution.expressions;

import main.catalog.Schema;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractExpression {
    private List<AbstractExpression> children;
    /** The return type of this expression. */
    private TypeID retType;

    public AbstractExpression(List<AbstractExpression> children, TypeID retType) {
        this.children = new ArrayList<>(children);
        this.retType = retType;
    }

    public abstract Value evaluate(Tuple tuple, Schema schema);

    public abstract Value evaluateJoin(Tuple left_tuple, Schema left_schema, Tuple right_tuple, Schema right_schema);

    public abstract Value evaluateAggregate(List<Value> groupBys, List<Value> aggregates);

    /** @return the child_idx'th child of this expression */
    public AbstractExpression getChildAt(int child_idx) {
        return children.get(child_idx);
    }

    /** @return the children of this expression, ordering may matter */
    public List<AbstractExpression> getChildren() {
        return children;
    }

    /** @return the type of this expression if it were to be evaluated */
    public TypeID getReturnType() {
        return retType;
    }

}

