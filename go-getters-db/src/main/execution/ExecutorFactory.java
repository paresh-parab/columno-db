package main.execution;

import main.edu.uci.db.execution.executors.AbstractExecutor;
import main.edu.uci.db.execution.executors.AggregationExecutor;
import main.edu.uci.db.execution.executors.InsertExecutor;
import main.edu.uci.db.execution.plans.AbstractPlanNode;
import main.edu.uci.db.execution.plans.AggregationPlanNode;
import main.edu.uci.db.execution.plans.InsertPlanNode;
import main.edu.uci.db.execution.plans.PlanType;

public class ExecutorFactory {
    static AbstractExecutor createExecutor(ExecutorContext execCtx, AbstractPlanNode plan) {
        switch (plan.getType()) {
            // Create a new sequential scan executor.
//            case PlanType.SeqScan: {
//                return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan));
//            }

            // Create a new insert executor.
            case Insert: {
                InsertPlanNode insertPlan = (InsertPlanNode) plan;
                AbstractExecutor child_executor =
                        insertPlan.isRawInsert() ? null : createExecutor(execCtx, insertPlan.getChildPlan());
                return new InsertExecutor(execCtx, insertPlan, child_executor);
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
                AbstractExecutor child_executor = createExecutor(execCtx, aggPlan.getChildPlan());
                return new AggregationExecutor(execCtx, aggPlan, child_executor);
            }

            default: {
                //USTUB_ASSERT(false, "Unsupported plan type.");
            }
        }
    }
}
