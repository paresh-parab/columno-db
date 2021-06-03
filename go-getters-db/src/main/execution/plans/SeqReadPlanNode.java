package main.execution.plans;

import main.catalog.Schema;
import main.execution.expressions.AbstractExpression;

import java.util.ArrayList;

import static main.common.Constants.DEBUGGER;
import static main.execution.plans.PlanType.SeqScan;

public class SeqReadPlanNode extends AbstractPlanNode{

    private AbstractExpression predicate;

    private int tableOID;

    public SeqReadPlanNode(Schema output, AbstractExpression predicate, int tableOID){
        super(output, new ArrayList<>());
        this.predicate = predicate;
        this.tableOID = tableOID;
        DEBUGGER.info("Initiated Sequential Read Plan with table ID :"+ tableOID);
    }

    @Override
    public PlanType getType() {
        return SeqScan;
    }

    public AbstractExpression getPredicate() {
        return predicate;
    }

    public int getTableOID() {
        return tableOID;
    }
}
