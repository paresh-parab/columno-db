package main.execution.expressions;

import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class AggregateValueExpression extends AbstractExpression{

    private boolean isGroupByTerm;
    private int termIdx;

    public AggregateValueExpression(boolean isGroupByTerm, int termIdx, TypeID ret_type) {
        super(new ArrayList<>(), ret_type);
        this.isGroupByTerm = isGroupByTerm;
        this.termIdx = termIdx;
    }

    @Override
    public Value evaluate(Tuple tuple, Schema schema) {
        try{
            throw new Exception("Aggregation should only refer to group-by and aggregates.");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
    }

    @Override
    public Value evaluateJoin(Tuple left_tuple, Schema left_schema, Tuple right_tuple, Schema right_schema) {
        try{
            throw new Exception("Aggregation should only refer to group-by and aggregates.");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

    @Override
    public Value evaluateAggregate(List<Value> groupBys, List<Value> aggregates) {
        return isGroupByTerm ? groupBys.get(termIdx) : aggregates.get(termIdx);
    }
}
