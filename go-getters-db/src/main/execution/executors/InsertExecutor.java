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
import static main.common.Constants.DEBUGGER;
import main.demo.Formatter;

import java.util.List;

public class InsertExecutor extends AbstractExecutor {

        private InsertPlanNode plan;
        private TableHeap table;
        private AbstractExecutor childExe;

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

        public boolean insertColVals(List<List<Value>> values, Catalog catalog, String tableName)
        {
                DEBUGGER.info("Invoking Insertion Executor");

                Tuple tuple = null;

                int numberOfTups = values.size();
                for (int idx = 0; idx < numberOfTups; idx++) {
                        tuple = new Tuple(values.get(idx));
                        DEBUGGER.info("Inserted following tuple into table :");
                        Formatter.prettyPrintTuple(tuple);
                }
                return true;
        }
}
