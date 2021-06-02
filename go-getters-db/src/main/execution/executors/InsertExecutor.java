package main.execution.executors;

import main.catalog.Catalog;
import main.catalog.Schema;
import main.demo.Formatter;
import main.execution.ExecutorContext;
import main.execution.plans.InsertPlanNode;
import main.storage.table.TableHeap;
import main.storage.table.Tuple;

import java.util.concurrent.ForkJoinPool;

import static main.common.Constants.DEBUGGER;

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
                DEBUGGER.info("Initializing Insertion Executor");

                Catalog catalog = this.getExecutorContext().getCatalog();
                this.table = catalog.getTable(this.plan.getTableOID()).getTable();
                // Initialize all children
                if (!this.plan.isRawInsert()) {
                        this.childExe.init();
                }

        }

        @Override
        public boolean next(Tuple[] tuples ) {
                // is raw insert -> get all raw tuples and insert
                DEBUGGER.info("Invoking Insertion Executor");

                Tuple tuple = null;
                if (this.plan.isRawInsert()) {
                        int numberOfTups = this.plan.getRawValues().size();
                        for (int idx = 0; idx < numberOfTups; idx++) {
                                tuple = new Tuple(this.plan.rawValuesAt(idx));
                                boolean success = this.table.insertTuple(tuple);
                                if (!success) return success;
                                DEBUGGER.info("Inserted following tuple into table :");
                                Formatter.prettyPrintTuple(tuple);
                        }
                        return true;
                }
                // get tuples from child executor, and then insert them
                while (this.childExe.next(tuples)) {
                        boolean success = this.table.insertTuple(tuple);
                        if (!success) return success;
                }
                return true;
        }

        @Override
        public Schema getOutputSchema() {
                Catalog catalog = this.getExecutorContext().getCatalog();
                return catalog.getTable(this.plan.getTableOID()).getSchema();
        }

}
