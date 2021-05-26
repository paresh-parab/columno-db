package main.execution.expressions;

import main.catalog.Schema;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class ColumnValueExpression extends AbstractExpression{
    private int tupleIdx;
    private int colIdx;

    public ColumnValueExpression(int tuple_idx, int col_idx, TypeID retType) {
        super(new ArrayList<>(), retType);
        this.tupleIdx = tupleIdx;
        this.colIdx = colIdx;
    }

    @Override
    public Value evaluate(Tuple tuple, Schema schema)  {
        return tuple.getValue(colIdx);
    }

    @Override
    public Value evaluateJoin(Tuple leftTuple, Schema leftSchema, Tuple rightTuple, Schema rightSchema)  {
        return tupleIdx == 0 ? leftTuple.getValue(colIdx) : rightTuple.getValue(colIdx);
    }

    @Override
    public Value evaluateAggregate(List<Value> groupBys, List<Value> aggregates)  {
        try{
            throw new Exception("Aggregation should only refer to group-by and aggregates.");
        }catch(Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

}
