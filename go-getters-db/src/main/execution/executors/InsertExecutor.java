package main.execution.executors;

import main.execution.ExecutorContext;
import main.execution.plans.InsertPlanNode;

public class InsertExecutor extends AbstractExecutor {

        private InsertPlanNode plan;
        private TableHeap table;
        private AbstractExecutor childExe;

        /**
         * Creates a new insert executor.
         *
         * @param exec_ctx       the executor context
         * @param plan           the insert plan to be executed
         * @param child_executor the child executor to obtain insert values from, can be nullptr
         */
        public InsertExecutor(ExecutorContext execCtx, InsertPlanNode plan, AbstractExecutor childExe){
                super(execCtx);
                this.plan = plan;
                this.childExe = childExe;
        }

        @Override
        public void init() {
                SimpleCatalog catalog = this.getExecutorContext().getCatalog();
                this.table = catalog.getTable(this.plan.getTableOID()).table.get();
                // Initialize all children
                if (!this.plan.isRawInsert()) {
                        this.childExe.init();
                }
        }

        @Override
        public boolean next(Tuple[] tuples ) {
                // is raw insert -> get all raw tuples and insert
                RID rid;
                Tuple tuple = null;
                if (this.plan.isRawInsert()) {
                        int numberOfTups = this.plan.getRawValues().size();
                        for (int idx = 0; idx < numberOfTups; idx++) {
                                tuple = new Tuple(this.plan.rawValuesAt(idx), this.getOutputSchema());
                                boolean success = this.table.insertTuple(tuple, rid, this.getExecutorContext().getTransaction());
                                // LOG_DEBUG("Insert tuple successfully: %s", tuple.ToString(this->GetOutputSchema()).c_str());
                                if (!success) return success;
                        }
                        // LOG_DEBUG("Insert %d tuples into table", int(numberOfTups));
                        return true;
                }
                // get tuples from child executor, and then insert them
                while (this.childExe.next(tuples)) {
                        // LOG_DEBUG("Read tuple: %s", tuple.ToString(this->GetOutputSchema()).c_str());
                        boolean success = this.table.insertTuple(tuple, rid, this.getExecutorContext().getTransaction());
                        if (!success) return success;
                }
                return true;
        }

        @Override
        public Schema getOutputSchema() {
                SimpleCatalog catalog = this.getExecutorContext().getCatalog();
                return catalog.getTable(this.plan.getTableOID()).schema;
        }

}
