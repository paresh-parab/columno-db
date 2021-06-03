package main.execution;

import main.execution.executors.AbstractExecutor;
import main.execution.executors.AggregationExecutor;
import main.execution.executors.InsertExecutor;
import main.execution.executors.SeqReadExecutor;
import main.execution.plans.*;

import static main.common.Constants.DEBUGGER;

public class ExecutorFactory {
    public static AbstractExecutor createExecutor(
            ExecutorContext execCtx, AbstractPlanNode plan, int mode) {
        switch (plan.getType()) {
            // Create a new sequential scan executor.
            case SeqScan: {
                if (mode == 0) {
                    SeqReadPlanNode readPlan = (SeqReadPlanNode) plan;
                    DEBUGGER.info("Creating Sequential Read Executor");
                    return new SeqReadExecutor(execCtx, readPlan);
                }
                else if(mode == 1){
                    SeqReadPlanNode readPlan = (SeqReadPlanNode) plan;
                    DEBUGGER.info("Creating Sequential Read Executor");
                    return new SeqReadExecutor(execCtx, readPlan);
                }
                else {
                }
            }


            case Insert: {
                if (mode == 0) {
                    InsertPlanNode insertPlan = (InsertPlanNode) plan;
                    AbstractExecutor child_executor =
                            insertPlan.isRawInsert() ? null : createExecutor(execCtx, insertPlan.getChildPlan(), 0);
                    DEBUGGER.info("Creating Insertion Executor");
                    return new InsertExecutor(execCtx, insertPlan, child_executor);
                }
                else if(mode == 1){
                    InsertPlanNode insertPlan = (InsertPlanNode) plan;
                    DEBUGGER.info("Creating Insertion Executor");
                    return new InsertExecutor(execCtx, insertPlan);
                }
                else{

                }
            }



            case Aggregation: {
                if (mode == 0) {
                    AggregationPlanNode aggPlan = (AggregationPlanNode) plan;
                    DEBUGGER.info("Creating Child Executor for Aggregation");
                    AbstractExecutor child_executor = createExecutor(execCtx, aggPlan.getChildPlan(), 0);
                    DEBUGGER.info("Creating Aggregate Read Executor");
                    return new AggregationExecutor(execCtx, aggPlan, child_executor);
                }
                else if(mode == 1)
                {
                    AggregationPlanNode aggPlan = (AggregationPlanNode) plan;
                    DEBUGGER.info("Creating Child Executor for Aggregation");
                    DEBUGGER.info("Creating Aggregate Read Executor");
                    return new AggregationExecutor(execCtx, aggPlan, null);
                }
                else{

                }
            }
            default: {
            }
        }
        return null;
    }
}
