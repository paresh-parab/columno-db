package main;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.ExecutorFactory;
import main.execution.executors.InsertExecutor;
import main.execution.plans.InsertPlanNode;
import main.storage.disk.DiskManager;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {

        List<Column> cols1 = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("Subscribed", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s1 = new Schema(cols1);
        String table1 = "SubscriptionRecord";

        List<Column> cols2 = new ArrayList<>(){{
            add(new Column("Name", TypeID.INTEGER_TYPE ));
            add(new Column("Age", TypeID.STRING_TYPE ));
            add(new Column("PhoneNo", TypeID.STRING_TYPE ));
        }};
        Schema s2 = new Schema(cols2);
        String table2 = "PhoneRecord";

        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new
                BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);
        catalog.createTable(table1, s1);
        catalog.createTable(table2, s2);

        ExecutorContext context = new ExecutorContext(catalog, bufferPoolManager);

        List<List<Value>> values = new ArrayList<>();
        values.add( new ArrayList<>(){{
            add(new Value(1));
            add(new Value("ABC"));
            add(new Value(true));
        }});

        values.add(new ArrayList<>(){{
            add(new Value(2));
            add(new Value("BCD"));
            add(new Value(true));
        }});

        values.add(new ArrayList<>(){{
            add(new Value(3));
            add(new Value("CDE"));
            add(new Value(true));
        }});

        values.add(new ArrayList<>(){{
            add(new Value(4));
            add(new Value("DEF"));
            add(new Value(false));
        }});


        InsertPlanNode pn = new InsertPlanNode(values, catalog.getTable(table1).getOid());

        InsertExecutor ex = (InsertExecutor) ExecutorFactory.createExecutor(context, pn);

        ex.init();
        ex.next(new Tuple[0]);

        System.out.println(catalog.getTable(table1).getTable().getColumnValues(0, TypeID.INTEGER_TYPE));
        System.out.println(catalog.getTable(table1).getTable().getColumnValues(1, TypeID.STRING_TYPE));


    }
}
