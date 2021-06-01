package main.execution.executors;

import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.storage.table.Tuple;

public abstract class AbstractExecutor {
    private ExecutorContext exeContext;

    public AbstractExecutor(){}

    public AbstractExecutor(ExecutorContext exeContext) {
        this.exeContext = exeContext;
    }

    public ExecutorContext getExecutorContext() {
        return exeContext;
    }

    public abstract void init() ;

    public abstract boolean next(Tuple[] tuple);

    public abstract Schema getOutputSchema() ;

}
