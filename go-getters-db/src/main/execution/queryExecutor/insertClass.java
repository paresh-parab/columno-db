package main.execution.queryExecutor;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.ExecutorFactory;
import main.execution.executors.InsertExecutor;
import main.execution.plans.InsertPlanNode;
import main.storage.disk.DiskManager;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class insertClass {

    public insertClass() {
    }

    List<Column> cols1 = new ArrayList<Column>(){{
        add(new Column("ID", TypeID.INTEGER_TYPE ));
        add(new Column("Name", TypeID.STRING_TYPE ));
        add(new Column("Subscribed", TypeID.BOOLEAN_TYPE ));
    }};
    Schema s1 = new Schema(cols1);
    String table1 = "SubscriptionRecord";

    List<Column> cols2 = new ArrayList<Column>(){{
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


    ExecutorContext context = new ExecutorContext(catalog, bufferPoolManager);

    ArrayList<List<Value>> values = new ArrayList<>();
    List<Value> tempList = new ArrayList<Value>(){{
        add(new Value(1));
        add(new Value("ABC"));
        add(new Value(true));
    }};

    List<Value> tempListTwo = new ArrayList<Value>(){{
        add(new Value(2));
        add(new Value("BCD"));
        add(new Value(true));
    }};

    List<Value> tempListThree = new ArrayList<Value>(){{
        add(new Value(3));
        add(new Value("CDE"));
        add(new Value(true));
    }};

    InsertPlanNode pn = new InsertPlanNode(values, catalog.getTable(table1).getOid());

    InsertExecutor ex = (InsertExecutor) ExecutorFactory.createExecutor(context, pn);


}
