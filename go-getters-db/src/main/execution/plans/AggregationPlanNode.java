package main.execution.plans;

import main.catalog.Schema;
import main.execution.expressions.AbstractExpression;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.DEBUGGER;

public class AggregationPlanNode extends AbstractPlanNode {

    private AbstractExpression having;
    private List<AbstractExpression> groupBys;
    private List<AbstractExpression> aggregates;
    private List<AggregationType> aggTypes;

    public AggregationPlanNode(Schema outputSchema, AbstractPlanNode child, AbstractExpression having,
                               List<AbstractExpression> groupBys,
                               List<AbstractExpression> aggregates, List<AggregationType> agg_types){
        super(outputSchema, new ArrayList<AbstractPlanNode>(){{
            add(child);
        }});
        this.having = having;
        this.groupBys = new ArrayList<>(groupBys);;
        this.aggregates = new ArrayList<>(aggregates);
        this.aggTypes = new ArrayList<>(agg_types);
        DEBUGGER.info("Initiated Aggregation Plan with group bys and aggregation types");
    }


    public AbstractExpression getHaving() {
        return having;
    }


    public AbstractExpression getGroupByAt(int idx)  {
        return groupBys.get(idx);
    }


    public List<AbstractExpression> getGroupBys() {
        return groupBys;
    }


    public AbstractExpression getAggregateAt(int idx) {
        return aggregates.get(idx);
    }


    public List<AbstractExpression> getAggregates() {
        return aggregates;
    }


    public List<AggregationType> getAggregateTypes() {
        return aggTypes;
    }

    @Override
    public PlanType getType() {
        return PlanType.Aggregation;
    }

    public AbstractPlanNode getChildPlan() {
        try {

            if (getChildren().size() != 1)
                throw new Exception("Aggregation expected to only have one child.");

            return getChildAt(0);

        }catch(Exception e){
                System.out.println("Program terminated due to exception: " + e.getMessage());
                System.out.println(e.getStackTrace());
                System.exit(0);
            }
        return null;
    }
}

