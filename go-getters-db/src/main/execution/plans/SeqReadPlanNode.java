package main.execution.plans;

import main.catalog.Schema;
import main.execution.expressions.AbstractExpression;

import java.util.ArrayList;

import static main.execution.plans.PlanType.SeqScan;

public class SeqReadPlanNode extends AbstractPlanNode{

    private AbstractExpression predicate;
    /** The table whose tuples should be scanned. */
    private int tableOID;

    public SeqReadPlanNode(Schema output, AbstractExpression predicate, int tableOID){
        super(output, new ArrayList<>());
        this.predicate = predicate;
        this.tableOID = tableOID;
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
