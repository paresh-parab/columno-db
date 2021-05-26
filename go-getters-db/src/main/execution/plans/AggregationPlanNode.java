package main.execution.plans;

import main.catalog.Schema;
import main.execution.expressions.AbstractExpression;

import java.util.ArrayList;
import java.util.List;

public class AggregationPlanNode extends AbstractPlanNode {

    private AbstractExpression having;
    List<AbstractExpression> groupBys;
    List<AbstractExpression> aggregates;
    List<AggregationType> aggTypes;

    public AggregationPlanNode(Schema output_schema, AbstractPlanNode child, AbstractExpression having,
                               List<AbstractExpression> group_bys,
                               List<AbstractExpression> aggregates, List<AggregationType> agg_types){
        super(output_schema, new ArrayList<AbstractPlanNode>(){{
            add(child);
        }});
        this.having = having;
        this.groupBys = group_bys;
        this.aggregates = new ArrayList<>(aggregates);
        this.aggTypes = new ArrayList<>(agg_types);
    }

    /** @return the having clause */
    public AbstractExpression getHaving() {
        return having;
    }

    /** @return the idx'th group by expression */
    public AbstractExpression getGroupByAt(int idx)  {
        return groupBys.get(idx);
    }

    /** @return the group by expressions */
    public List<AbstractExpression> getGroupBys() {
        return groupBys;
    }

    /** @return the idx'th aggregate expression */
    public AbstractExpression getAggregateAt(int idx) {
        return aggregates.get(idx);
    }

    /** @return the aggregate expressions */
    public List<AbstractExpression> getAggregates() {
        return aggregates;
    }

    /** @return the aggregate types */
    public List<AggregationType> getAggregateTypes() {
        return aggTypes;
    }

    @Override
    public PlanType getType() {
        return PlanType.Aggregation;
    }

    public AbstractPlanNode getChildPlan() {
        try {

            if (getChildren().size() == 1)
                throw new Exception("Aggregation expected to only have one child.");
            return getChildAt(0);
        }catch(Exception e){
                System.out.println("Program terminated due to exception: "+ e.getMessage());
                System.out.println(e.getStackTrace());
                System.exit(0);
            }
        return null;
    }
}

