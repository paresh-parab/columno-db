package main.execution.expressions;

import main.catalog.Schema;
import main.storage.table.Tuple;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class ConstantValueExpression extends  AbstractExpression {

    private Value val;


    public ConstantValueExpression(Value val) {
        super(new ArrayList<AbstractExpression>(), val.getTypeID());
        this.val = val;
    }

    @Override
    public Value evaluate(Tuple tuple, Schema schema) {
        return val;
    }

    @Override
    public Value evaluateJoin(Tuple left_tuple, Schema left_schema, Tuple right_tuple, Schema right_schema) {
        return val;
    }

    @Override
    public Value evaluateAggregate(List<Value> group_bys, List<Value> aggregates) {
        return val;
    }
}
