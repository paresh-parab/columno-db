package main.execution.executors;

import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.storage.table.Tuple;

public abstract class AbstractExecutor {
    private ExecutorContext exeContext;

    public AbstractExecutor(ExecutorContext exeContext) {
        this.exeContext = exeContext;
    }

    public ExecutorContext getExecutorContext() {
        return exeContext;
    }

    public abstract void init() ;

    /**
     * Produces the next tuple from this executor.
     * @param[out] tuple the next tuple produced by this executor
     * @return true if a tuple was produced, false if there are no more tuples
     */
    public abstract boolean next(Tuple[] tuple);

    /** @return the schema of the tuples that this executor produces */
    public abstract Schema getOutputSchema() ;

}
