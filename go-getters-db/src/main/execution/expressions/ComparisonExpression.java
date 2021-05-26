package main.execution.expressions;

import main.catalog.Schema;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class ComparisonExpression extends AbstractExpression {

    private List<AbstractExpression> children;
    private ComparisonType comparisonType;

    public ComparisonExpression(AbstractExpression left, AbstractExpression right, ComparisonType comp_type) {
        super(new ArrayList<AbstractExpression>() {{
            add(left);
            add(right);
        }}, TypeID.INTEGER_TYPE); //should be boolean
        comparisonType = comp_type;
    }


    @Override
    public Value evaluate(Tuple tuple, Schema schema)  {
        Value lhs = getChildAt(0).evaluate(tuple, schema);
        Value rhs = getChildAt(1).evaluate(tuple, schema);
        return new Value(performComparison(lhs, rhs));
    }

    @Override
    public Value evaluateJoin(Tuple left_tuple, Schema left_schema, Tuple right_tuple, Schema right_schema) {
        Value lhs = getChildAt(0).evaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
        Value rhs = getChildAt(1).evaluateJoin(left_tuple, left_schema, right_tuple, right_schema);
        return new Value(performComparison(lhs, rhs));

    }

    @Override
    public Value evaluateAggregate(List<Value> group_bys, List<Value> aggregates)  {
        Value lhs = getChildAt(0).evaluateAggregate(group_bys, aggregates);
        Value rhs = getChildAt(1).evaluateAggregate(group_bys, aggregates);
        return new Value(performComparison(lhs, rhs));
    }

    private boolean performComparison(Value lhs, Value rhs) {
        try{
            switch (comparisonType) {
                case Equal:
                    return lhs.compareEquals(rhs);
                case NotEqual:
                    return lhs.compareNotEquals(rhs);
                case LessThan:
                    return lhs.compareLessThan(rhs);
                case LessThanOrEqual:
                    return lhs.compareLessThanEquals(rhs);
                case GreaterThan:
                    return lhs.compareGreaterThan(rhs);
                case GreaterThanOrEqual:
                    return lhs.compareGreaterThanEquals(rhs);
                default:
                    throw new Exception("Unsupported comparison type.");
            }
        }catch (Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return false;
    }


}