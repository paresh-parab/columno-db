package main.execution.executors;

import main.catalog.Catalog;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.plans.SeqReadPlanNode;
import main.storage.table.TableHeap;
import main.storage.table.Tuple;

import java.util.Iterator;
import java.util.List;

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
        Catalog catalog = getExecutorContext().getCatalog();
        this.table = catalog.getTable(this.plan.getTableOID()).getTable();
    }

    @Override
    public boolean next(Tuple[] tuple) {
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
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema getOutputSchema() {
        return plan.getOutputSchema();
    }
}
