package test.execution;

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

import static org.junit.jupiter.api.Assertions.assertEquals;

public class InsertExecutorTest {

    @org.junit.jupiter.api.Test
    void insertionTest() {
        List<Column> cols1 = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("Subscribed", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s1 = new Schema(cols1);
        String table1 = "SubscriptionRecord";

        List<Column> cols2 = new ArrayList<>(){{
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("Age", TypeID.INTEGER_TYPE ));
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

        List<List<Value>> values2 = new ArrayList<>();
        values2.add( new ArrayList<>(){{
            add(new Value("ABC"));
            add(new Value(20));
            add(new Value("+123"));
        }});

        values2.add( new ArrayList<>(){{
            add(new Value("ABC"));
            add(new Value(20));
            add(new Value("+1234"));
        }});

        values2.add( new ArrayList<>(){{
            add(new Value("BCD"));
            add(new Value(22));
            add(new Value("+1352"));
        }});

        values2.add( new ArrayList<>(){{
            add(new Value("XYZ"));
            add(new Value(22));
            add(new Value("+1952"));
        }});

        values2.add( new ArrayList<>(){{
            add(new Value("BCD"));
            add(new Value(22));
            add(new Value("+1452"));
        }});


        InsertPlanNode ipn1 = new InsertPlanNode(values, catalog.getTable(table1).getOid());
        InsertExecutor ex1 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn1, 0);
        ex1.init();
        ex1.next(new Tuple[0]);

        InsertPlanNode ipn2 = new InsertPlanNode(values2, catalog.getTable(table2).getOid());
        InsertExecutor ex2 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn2, 0);
        ex2.init();
        ex2.next(new Tuple[0]);

        assertEquals( "1\tABC\ttrue", catalog.getTable(table1).getTable().readAllRows().get(0).toString());
        assertEquals("2\tBCD\ttrue" , catalog.getTable(table1).getTable().readAllRows().get(1).toString());
        assertEquals( "3\tCDE\ttrue", catalog.getTable(table1).getTable().readAllRows().get(2).toString());
        assertEquals( "4\tDEF\tfalse", catalog.getTable(table1).getTable().readAllRows().get(3).toString());


        assertEquals( "ABC\t20\t+123", catalog.getTable(table2).getTable().readAllRows().get(0).toString());
        assertEquals("ABC\t20\t+1234" , catalog.getTable(table2).getTable().readAllRows().get(1).toString());
        assertEquals( "BCD\t22\t+1352", catalog.getTable(table2).getTable().readAllRows().get(2).toString());
        assertEquals( "XYZ\t22\t+1952", catalog.getTable(table2).getTable().readAllRows().get(3).toString());
    }
}
