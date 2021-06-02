package main.execution.plans;

import main.catalog.Schema;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractPlanNode {

    private Schema outputSchema;
    /** The children of this plan node. */
    private List<AbstractPlanNode> children;

    public AbstractPlanNode(){}

    public AbstractPlanNode(Schema output_schema, List<AbstractPlanNode> children){
        this.outputSchema = output_schema;
        this.children = new ArrayList<>(children);
    }

    public Schema getOutputSchema() {
        return outputSchema;
    }

    /** @return the child of this plan node at index child_idx */
    public AbstractPlanNode getChildAt(int childIdx){
        return children.get(childIdx);
    }

    /** @return the children of this plan node */
    public List<AbstractPlanNode> getChildren() {
        return children;
    }

    /** @return the type of this plan node */
    public abstract PlanType getType();

}