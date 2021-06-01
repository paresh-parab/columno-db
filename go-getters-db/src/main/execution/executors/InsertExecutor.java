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
                Tuple tuple = null;
                if (this.plan.isRawInsert()) {
                        int numberOfTups = this.plan.getRawValues().size();
                        for (int idx = 0; idx < numberOfTups; idx++) {
                                tuple = new Tuple(this.plan.rawValuesAt(idx));
                                boolean success = this.table.insertTuple(tuple);
                                // LOG_DEBUG("Insert tuple successfully: %s", tuple.ToString(this->GetOutputSchema()).c_str());
                                if (!success) return success;
                        }
                        // LOG_DEBUG("Insert %d tuples into table", int(numberOfTups));
                        return true;
                }
                // get tuples from child executor, and then insert them
                while (this.childExe.next(tuples)) {
                        // LOG_DEBUG("Read tuple: %s", tuple.ToString(this->GetOutputSchema()).c_str());
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
