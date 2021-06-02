package main.execution.plans;

import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.DEBUGGER;

public class InsertPlanNode extends AbstractPlanNode {
    List<List<Value>> rawValues;

    int tableOID;

    public InsertPlanNode(List<List<Value>> rawValues, int tableOID) {
        super(null, new ArrayList<>());
        this.rawValues = new ArrayList<>(rawValues);
        this.tableOID = tableOID;
        DEBUGGER.info("Initiated Insert Plan with Table ID :"+ tableOID);
    }

    public InsertPlanNode(List<List<Value>> rawValues) {}

    public InsertPlanNode(AbstractPlanNode child, int tableOID) {
        super(null, new ArrayList<>() {{
            add(child);
        }});
        this.tableOID = tableOID;
        DEBUGGER.info("Initiated Insert Plan with Table ID :"+ tableOID);
    }


    @Override
    public PlanType getType() {
        return PlanType.Insert;
    }

    public int getTableOID() {
        return tableOID;
    }

    public boolean isRawInsert() {
        return getChildren().isEmpty();
    }

    public List<Value> rawValuesAt(int idx) {
        return rawValues.get(idx);
    }


    public List<List<Value>> getRawValues() {
        return rawValues;
    }

    public AbstractPlanNode getChildPlan() {
        return getChildAt(0);
    }

}