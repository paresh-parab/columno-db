package main.execution.executors;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.plans.InsertPlanNode;
import main.storage.page.Page;
import main.storage.page.TablePage;
import main.storage.table.TableHeap;
import main.storage.table.Tuple;
import main.type.PageType;
import main.type.Value;
import static java.lang.System.exit;

import java.util.List;

import static main.common.Constants.DEBUGGER;

public class InsertExecutor extends AbstractExecutor {

        private InsertPlanNode plan;
        private TableHeap table;
        private AbstractExecutor childExe;

        /**
         * Creates a new insert executor.
         *
         * @param execCtx       the executor context
         * @param plan           the insert plan to be executed
         * @param childExe the child executor to obtain insert values from, can be nullptr
         */
        public InsertExecutor(ExecutorContext execCtx, InsertPlanNode plan, AbstractExecutor childExe){
                super(execCtx);
                this.plan = plan;
                this.childExe = childExe;
        }

        public InsertExecutor(ExecutorContext executorContext, InsertPlanNode plan){}

        @Override
        public void init() {
                DEBUGGER.info("Initializing Insertion Executor");

                Catalog catalog = this.getExecutorContext().getCatalog();
                this.table = catalog.getTable(this.plan.getTableOID()).getTable();
                // Initialize all children
                if (!this.plan.isRawInsert()) {
                        this.childExe.init();
                }
                DEBUGGER.info("Succesfully initialized Insertion Executor");

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
                                DEBUGGER.info("Inserted following tuple into table :"+ tuple.toString());
                        }
                        DEBUGGER.info("Inserted "+ numberOfTups + " tuples into table");
                        DEBUGGER.info("Exiting Insertion Executor");
                        return true;
                }
                // get tuples from child executor, and then insert them
                while (this.childExe.next(tuples)) {
                        boolean success = this.table.insertTuple(tuple);
                        if (!success) return success;
                }
                DEBUGGER.info("Exiting Insertion Executor");
                return true;
        }

        @Override
        public Schema getOutputSchema() {
                Catalog catalog = this.getExecutorContext().getCatalog();
                return catalog.getTable(this.plan.getTableOID()).getSchema();
        }

        public TablePage insertColStore(List<List<Value>> values, Catalog catalog, String tableName)
        {
                try
                {
                        int colStart = catalog.getNames().get(tableName);
                        TablePage tp = new TablePage();
                        tp.tableName = tableName;
                        for (List<Value> value : values) {
                                tp.colData.put(catalog.colHeap.get(colStart++), value);
                        }

                        return tp;
                } catch (Exception e) {
                        System.out.println("Exception: Error in inserting values in columnar style");
                        e.printStackTrace();
                        exit(1);
                }
                return null;
        }
}
