package main.execution.catalog;

public abstract class executorAbs {
    private ExContext exeContext;

    public executorAbs(ExContext exeContext) {
        this.exeContext = exeContext;
    }

    public ExContext getExecutorContext() {
        return exeContext;
    }

    public abstract void init() ;

    public abstract Schema getOutputSchema() ;
}
