package main.execution;

import main.execution.executors.AbstractExecutor;
import main.execution.executors.AggregationExecutor;
import main.execution.executors.InsertExecutor;
import main.execution.executors.SeqReadExecutor;
import main.execution.plans.*;

import static main.common.Constants.DEBUGGER;

public class ExecutorFactory {
    public static AbstractExecutor createExecutor(ExecutorContext execCtx, AbstractPlanNode plan) {
        switch (plan.getType()) {
            // Create a new sequential scan executor.
            case SeqScan:{
                SeqReadPlanNode readPlan = (SeqReadPlanNode) plan;
                DEBUGGER.info("Creating Sequential Read Executor");
                return new SeqReadExecutor(execCtx, readPlan);
            }

            // Create a new insert executor.
            case Insert: {
                InsertPlanNode insertPlan = (InsertPlanNode) plan;
                AbstractExecutor child_executor =
                        insertPlan.isRawInsert() ? null : createExecutor(execCtx, insertPlan.getChildPlan());
                DEBUGGER.info("Creating Insertion Executor");
                return new InsertExecutor(execCtx, insertPlan, child_executor);
            }


            // Create a new aggregation executor.
            case Aggregation: {
                AggregationPlanNode aggPlan = (AggregationPlanNode)plan ;
                DEBUGGER.info("Creating Child Executor for Aggregation");
                AbstractExecutor child_executor = createExecutor(execCtx, aggPlan.getChildPlan());
                DEBUGGER.info("Creating Aggregate Read Executor");
                return new AggregationExecutor(execCtx, aggPlan, child_executor);
            }
            default: {
            }
        }
        return null;
    }
}
