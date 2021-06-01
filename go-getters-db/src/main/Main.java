package main;

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
import main.storage.page.Page;
import main.storage.page.TablePage;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args)
    {
        // Row store insertion

//        List<Column> cols1 = new ArrayList<>(){{
//            add(new Column("ID", TypeID.INTEGER_TYPE ));
//            add(new Column("Name", TypeID.STRING_TYPE ));
//            add(new Column("Subscribed", TypeID.BOOLEAN_TYPE ));
//        }};
//        Schema s1 = new Schema(cols1);
//        String table1 = "SubscriptionRecord";
//
//        List<Column> cols2 = new ArrayList<>(){{
//            add(new Column("Name", TypeID.STRING_TYPE ));
//            add(new Column("Age", TypeID.INTEGER_TYPE ));
//            add(new Column("PhoneNo", TypeID.STRING_TYPE ));
//        }};
//        Schema s2 = new Schema(cols2);
//        String table2 = "PhoneRecord";
//
//        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");
//
//        BufferPoolManager bufferPoolManager = new
//                BufferPoolManager(10, diskManager);
//
//        Catalog catalog = new Catalog(bufferPoolManager);
//        catalog.createTable(table1, s1);
//        catalog.createTable(table2, s2);
//
//        ExecutorContext context = new ExecutorContext(catalog, bufferPoolManager);
//
//        List<List<Value>> values = new ArrayList<>();
//        values.add( new ArrayList<>(){{
//            add(new Value(1));
//            add(new Value("ABC"));
//            add(new Value(true));
//        }});
//
//        values.add(new ArrayList<>(){{
//            add(new Value(2));
//            add(new Value("BCD"));
//            add(new Value(true));
//        }});
//
//        values.add(new ArrayList<>(){{
//            add(new Value(3));
//            add(new Value("CDE"));
//            add(new Value(true));
//        }});
//
//        values.add(new ArrayList<>(){{
//            add(new Value(4));
//            add(new Value("DEF"));
//            add(new Value(false));
//        }});
//
//        List<List<Value>> values2 = new ArrayList<>();
//        values2.add( new ArrayList<>(){{
//            add(new Value("ABC"));
//            add(new Value(20));
//            add(new Value("+123"));
//        }});
//
//        values2.add( new ArrayList<>(){{
//            add(new Value("ABC"));
//            add(new Value(20));
//            add(new Value("+1234"));
//        }});
//
//        values2.add( new ArrayList<>(){{
//            add(new Value("BCD"));
//            add(new Value(22));
//            add(new Value("+1352"));
//        }});
//
//        values2.add( new ArrayList<>(){{
//            add(new Value("XYZ"));
//            add(new Value(22));
//            add(new Value("+1952"));
//        }});
//
//        values2.add( new ArrayList<>(){{
//            add(new Value("BCD"));
//            add(new Value(22));
//            add(new Value("+1452"));
//        }});
//
//
//        InsertPlanNode ipn1 = new InsertPlanNode(values, catalog.getTable(table1).getOid());
//        InsertExecutor ex1 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn1);
//        ex1.init();
//        ex1.next(new Tuple[0]);
//
//        InsertPlanNode ipn2 = new InsertPlanNode(values2, catalog.getTable(table2).getOid());
//        InsertExecutor ex2 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn2);
//        ex2.init();
//        ex2.next(new Tuple[0]);

//        System.out.println(catalog.getTable(table1).getTable().getColumnValues(0, TypeID.INTEGER_TYPE));
//        System.out.println(catalog.getTable(table1).getTable().getColumnValues(1, TypeID.STRING_TYPE));
//
//        System.out.println(catalog.getTable(table2).getTable().getColumnValues(0, TypeID.STRING_TYPE));
//        System.out.println(catalog.getTable(table2).getTable().getColumnValues(1, TypeID.INTEGER_TYPE));

// ***********************************************************************************************************

// Column Store Insertion

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

        DiskManager diskManager = new DiskManager("SampleCol.db");

        BufferPoolManager bufferPoolManager = new
                BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);
        catalog.createTableCol(table1, s1);
        catalog.createTableCol(table2, s2);

        ExecutorContext context = new ExecutorContext(catalog, bufferPoolManager);

        List<List<Value>> values = new ArrayList<>();
        values.add( new ArrayList<>(){{
            add(new Value(1));
            add(new Value(2));
            add(new Value(3));
            add(new Value(4));
        }});

        values.add(new ArrayList<>(){{
            add(new Value("ABC"));
            add(new Value("BCD"));
            add(new Value("CDE"));
            add(new Value("DEF"));
        }});

        values.add(new ArrayList<>(){{
            add(new Value(true));
            add(new Value(true));
            add(new Value(true));
            add(new Value(false));
        }});

        InsertPlanNode ipn1 = new InsertPlanNode(values);
        InsertExecutor ex1 = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn1, 1);
        TablePage insertCol;
        insertCol = ex1.insertColStore(values, catalog, table1);

        System.out.println("***********************************");
        System.out.println("Table Name: " + insertCol.tableName);

        for(String key : insertCol.colData.keySet()) {
            System.out.println(key + ": " + insertCol.colData.get(key));
        }
        System.out.println("*************************************");



//        System.out.println(catalog.getTable(table2).getTable().getColumnValues(0, TypeID.STRING_TYPE));
//        System.out.println(catalog.getTable(table2).getTable().getColumnValues(1, TypeID.INTEGER_TYPE));

// ##################             AggregationExecutor           #############################
//        List<Column> outputCols = new ArrayList<>(){{
//            add(new Column("MAX", TypeID.INTEGER_TYPE, new AggregateValueExpression(false,0, TypeID.INTEGER_TYPE) ));
//        }};
//
//        ColumnValueExpression ce = new ColumnValueExpression(-1,0, TypeID.INTEGER_TYPE);
//
//        SeqReadPlanNode readPlanNode = new SeqReadPlanNode(s1, ce, catalog.getTable(table1).getOid());
//
//        Schema outputSchema = new Schema(outputCols);
//        AggregationPlanNode apn = new AggregationPlanNode(outputSchema,readPlanNode,
//                 null,
//                 new ArrayList<AbstractExpression>(),
//                 new ArrayList<>(){{
//                     add(new ColumnValueExpression(-1,0, TypeID.INTEGER_TYPE) );
//                 }},
//                 new ArrayList<>(){{
//            add(AggregationType.MinAggregate);
//        }});
//
//
//        AggregationExecutor aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn);
//        aex.init();
//        Tuple[] res = new Tuple[1];
//        aex.next(res);

//#################### AggregationExecutor2 ###########################

//        List<Column> outputCols2 = new ArrayList<>(){{
//            add(new Column("Name",
//                    TypeID.STRING_TYPE,
//                    new AggregateValueExpression(true,0, TypeID.STRING_TYPE) ));
//            add(new Column("Count",
//                    TypeID.INTEGER_TYPE,
//                    new AggregateValueExpression(false,0, TypeID.INTEGER_TYPE) ));
//        }};
//
//        ColumnValueExpression ce2 = new ColumnValueExpression(-1,2, TypeID.STRING_TYPE);
//
//        SeqReadPlanNode readPlanNode2 = new SeqReadPlanNode(s2, ce2, catalog.getTable(table2).getOid());
//
//        Schema outputSchema2 = new Schema(outputCols2);
//        AggregationPlanNode apn2 = new AggregationPlanNode(outputSchema2,
//                readPlanNode2,
//                null,
//                new ArrayList<>(){{
//                    add(new ColumnValueExpression(-1,0, TypeID.STRING_TYPE) );
//                }},
//                new ArrayList<>(){{
//                    add(new ColumnValueExpression(-1,2, TypeID.STRING_TYPE) );
//                }},
//                new ArrayList<>(){{
//                    add(AggregationType.CountAggregate);
//                }});
//
//
//        AggregationExecutor aex2 = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn2);
//        aex2.init();
//        Tuple[] res2 = new Tuple[1];
//        aex2.next(res2);
//
//        System.out.println(res2[0]);
//
//        aex2.next(res2);
//
//        System.out.println(res2[0]);aex2.next(res2);
//
//        System.out.println(res2[0]);

    }
}
