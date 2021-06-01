package main.execution;

import main.execution.executors.AbstractExecutor;
import main.execution.executors.AggregationExecutor;
import main.execution.executors.InsertExecutor;
import main.execution.executors.SeqReadExecutor;
import main.execution.plans.*;

public class ExecutorFactory {
    public static AbstractExecutor createExecutor(ExecutorContext execCtx, AbstractPlanNode plan, int mode) {
        switch (plan.getType()) {
            // Create a new sequential scan executor.
            case SeqScan:{
                if(mode == 0) {
                    SeqReadPlanNode readPlan = (SeqReadPlanNode) plan;
                    return new SeqReadExecutor(execCtx, readPlan);
                }
            }

            // Create a new insert executor.
            case Insert: {
                if (mode == 0) {
                    InsertPlanNode insertPlan = (InsertPlanNode) plan;
                    AbstractExecutor child_executor =
                            insertPlan.isRawInsert() ? null : createExecutor(execCtx, insertPlan.getChildPlan(), 0);
                    return new InsertExecutor(execCtx, insertPlan, child_executor);
                }
                else{
                    InsertPlanNode insertPlan = (InsertPlanNode) plan;
                    return new InsertExecutor(execCtx, insertPlan);
                }
            }

//            // Create a new hash join executor.
//            case PlanType::HashJoin: {
//                auto join_plan = dynamic_cast<const HashJoinPlanNode *>(plan);
//                auto left_executor = ExecutorFactory::CreateExecutor(exec_ctx, join_plan->GetLeftPlan());
//                auto right_executor = ExecutorFactory::CreateExecutor(exec_ctx, join_plan->GetRightPlan());
//                return std::make_unique<HashJoinExecutor>(exec_ctx, join_plan, std::move(left_executor),
//                        std::move(right_executor));
//            }

            // Create a new aggregation executor.
            case Aggregation: {
                AggregationPlanNode aggPlan = (AggregationPlanNode)plan ;
                AbstractExecutor child_executor = createExecutor(execCtx, aggPlan.getChildPlan(), 0);
                return new AggregationExecutor(execCtx, aggPlan, child_executor);
            }

            default: {
                //USTUB_ASSERT(false, "Unsupported plan type.");
            }
        }
        return null;
    }
}
