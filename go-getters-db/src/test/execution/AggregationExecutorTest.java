package test.execution;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.execution.ExecutorContext;
import main.execution.ExecutorFactory;
import main.execution.executors.AggregationExecutor;
import main.execution.executors.InsertExecutor;
import main.execution.expressions.AbstractExpression;
import main.execution.expressions.AggregateValueExpression;
import main.execution.expressions.ColumnValueExpression;
import main.execution.plans.AggregationPlanNode;
import main.execution.plans.AggregationType;
import main.execution.plans.InsertPlanNode;
import main.execution.plans.SeqReadPlanNode;
import main.storage.disk.DiskManager;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AggregationExecutorTest {

    @org.junit.jupiter.api.Test
    void simpleAggTest() {

        List<Column> cols1 = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("Subscribed", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s1 = new Schema(cols1);
        String table1 = "SubscriptionRecord";
        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new
                BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);
        catalog.createTable(table1, s1);

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

        InsertPlanNode ipn1 = new InsertPlanNode(values, catalog.getTable(table1).getOid());
        InsertExecutor ex1 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn1, 0);
        ex1.init();
        ex1.next(new Tuple[0]);

        List<Column> outputCols = new ArrayList<>() {{
            add(new Column("MAX", TypeID.INTEGER_TYPE, new AggregateValueExpression(false, 0, TypeID.INTEGER_TYPE)));
        }};

        ColumnValueExpression ce = new ColumnValueExpression(-1, 0, TypeID.INTEGER_TYPE);

        SeqReadPlanNode readPlanNode = new SeqReadPlanNode(s1, ce, catalog.getTable(table1).getOid());

        Schema outputSchema = new Schema(outputCols);
        AggregationPlanNode apn = new AggregationPlanNode(outputSchema, readPlanNode,
                null,
                new ArrayList<AbstractExpression>(),
                new ArrayList<>() {{
                    add(new ColumnValueExpression(-1, 0, TypeID.INTEGER_TYPE));
                }},
                new ArrayList<>() {{
                    add(AggregationType.CountAggregate);
                }});


        AggregationExecutor aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn, 0);
        aex.init();
        Tuple[] res = new Tuple[1];
        aex.next(res);
        assertEquals( "4", res[0].toString());

        apn = new AggregationPlanNode(outputSchema, readPlanNode,
                null,
                new ArrayList<AbstractExpression>(),
                new ArrayList<>() {{
                    add(new ColumnValueExpression(-1, 0, TypeID.INTEGER_TYPE));
                }},
                new ArrayList<>() {{
                    add(AggregationType.MinAggregate);
                }});


        aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn, 0);
        aex.init();
        res = new Tuple[1];
        aex.next(res);
        assertEquals( "1", res[0].toString());

        apn = new AggregationPlanNode(outputSchema, readPlanNode,
                null,
                new ArrayList<AbstractExpression>(),
                new ArrayList<>() {{
                    add(new ColumnValueExpression(-1, 0, TypeID.INTEGER_TYPE));
                }},
                new ArrayList<>() {{
                    add(AggregationType.MaxAggregate);
                }});


        aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn, 0);
        aex.init();
        res = new Tuple[1];
        aex.next(res);
        assertEquals( "4", res[0].toString());

        apn = new AggregationPlanNode(outputSchema, readPlanNode,
                null,
                new ArrayList<AbstractExpression>(),
                new ArrayList<>() {{
                    add(new ColumnValueExpression(-1, 0, TypeID.INTEGER_TYPE));
                }},
                new ArrayList<>() {{
                    add(AggregationType.SumAggregate);
                }});


        aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn, 0);
        aex.init();
        res = new Tuple[1];
        aex.next(res);
        assertEquals( "10", res[0].toString());

    }

    @org.junit.jupiter.api.Test
    void groupByTest(){

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
        catalog.createTable(table2, s2);

        ExecutorContext context = new ExecutorContext(catalog, bufferPoolManager);

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

        InsertPlanNode ipn2 = new InsertPlanNode(values2, catalog.getTable(table2).getOid());
        InsertExecutor ex2 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn2, 0);
        ex2.init();
        ex2.next(new Tuple[0]);


        List<Column> outputCols2 = new ArrayList<>(){{
            add(new Column("Name",
                    TypeID.STRING_TYPE,
                    new AggregateValueExpression(true,0, TypeID.STRING_TYPE) ));
            add(new Column("Count",
                    TypeID.INTEGER_TYPE,
                    new AggregateValueExpression(false,0, TypeID.INTEGER_TYPE) ));
        }};

        ColumnValueExpression ce2 = new ColumnValueExpression(-1,2, TypeID.STRING_TYPE);

        SeqReadPlanNode readPlanNode2 = new SeqReadPlanNode(s2, ce2, catalog.getTable(table2).getOid());

        Schema outputSchema2 = new Schema(outputCols2);
        AggregationPlanNode apn2 = new AggregationPlanNode(outputSchema2,
                readPlanNode2,
                null,
                new ArrayList<>(){{
                    add(new ColumnValueExpression(-1,0, TypeID.STRING_TYPE) );
                }},
                new ArrayList<>(){{
                    add(new ColumnValueExpression(-1,2, TypeID.STRING_TYPE) );
                }},
                new ArrayList<>(){{
                    add(AggregationType.CountAggregate);
                }});


        AggregationExecutor aex2 = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn2, 0);
        aex2.init();
        Tuple[] res2 = new Tuple[1];
        aex2.next(res2);

        assertEquals("XYZ\t1", res2[0].toString());
        aex2.next(res2);
        assertEquals("ABC\t2", res2[0].toString());
        aex2.next(res2);
        assertEquals("BCD\t2", res2[0].toString());    }
}
