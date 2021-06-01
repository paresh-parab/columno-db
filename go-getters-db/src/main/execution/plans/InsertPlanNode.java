package main.execution.plans;

import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.DEBUGGER;

public class InsertPlanNode extends AbstractPlanNode {
    List<List<Value>> rawValues;
    /**
     * The table to be inserted into.
     */
    int tableOID;

    /**
     * Creates a new insert plan node for inserting raw values.
     *
     * @param raw_values the raw values to be inserted
     * @param table_oid  the identifier of the table to be inserted into
     */

    public InsertPlanNode(List<List<Value>> rawValues, int tableOID) {
        super(null, new ArrayList<>());
        this.rawValues = new ArrayList<>(rawValues);
        this.tableOID = tableOID;
        DEBUGGER.info("Initiated Insert Plan with Table ID :"+ tableOID);
    }

    /**
     * Creates a new insert plan node for inserting values from a child plan.
     *
     * @param child     the child plan to obtain values from
     * @param table_oid the identifier of the table that should be inserted into
     */
    public InsertPlanNode(AbstractPlanNode child, int tableOID) {
        super(null, new ArrayList<AbstractPlanNode>() {{
            add(child);
        }});
        this.tableOID = tableOID;
        DEBUGGER.info("Initiated Insert Plan with Table ID :"+ tableOID);
    }


    @Override
    public PlanType getType() {
        return PlanType.Insert;
    }

    /**
     * @return the identifier of the table that should be inserted into
     */
    public int getTableOID() {
        return tableOID;
    }

    /**
     * @return true if we embed insert values directly into the plan, false if we have a child plan providing tuples
     */
    public boolean isRawInsert() {
        return getChildren().isEmpty();
    }

    /**
     * @return the raw values to be inserted at the particular index
     */
    public List<Value> rawValuesAt(int idx) {
        return rawValues.get(idx);
    }

    /**
     * @return the raw values to be inserted
     */
    public List<List<Value>> getRawValues() {
        return rawValues;
    }

    /**
     * @return the child plan providing tuples to be inserted
     */
    public AbstractPlanNode getChildPlan() {
        return getChildAt(0);
    }

}