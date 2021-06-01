package main.execution.executors;

import main.catalog.Catalog;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.plans.SeqReadPlanNode;
import main.storage.table.TableHeap;
import main.storage.table.Tuple;

import java.util.Iterator;
import java.util.List;

import static main.common.Constants.DEBUGGER;

public class SeqReadExecutor extends AbstractExecutor{

    private SeqReadPlanNode plan;
    private TableHeap table;
    private int iter;

    public SeqReadExecutor(ExecutorContext execCtx, SeqReadPlanNode plan) {
        super(execCtx);
        this.plan = plan;
        this.iter = 0;
    }

    @Override
    public void init() {
        DEBUGGER.info("Initializing Sequential Read Executor");

        Catalog catalog = getExecutorContext().getCatalog();
        this.table = catalog.getTable(this.plan.getTableOID()).getTable();

        DEBUGGER.info("Succesfully initialized Sequential Read Executor");

    }

    @Override
    public boolean next(Tuple[] tuple) {
        DEBUGGER.info("Invoking Sequential Read Executor");

        List<Tuple> data = table.readAllRows();
        for(int i = iter; i < data.size(); i++){
            iter++;
            Tuple t = data.get(i);
            boolean eval = true;
            if (this.plan.getPredicate() != null) {
                eval = this.plan.getPredicate().evaluate(t, this.getOutputSchema()).getAsBoolean();
            }
            if (eval) {
                tuple[0] = new Tuple(t);
                DEBUGGER.info("Following tuple satisfies the predicate "+ tuple[0].toString());
                DEBUGGER.info("Returning this tuple from Sequential Read Executor");
                return true;
            }
        }
        DEBUGGER.info("Exiting Sequential Read Executor");
        return false;
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}
