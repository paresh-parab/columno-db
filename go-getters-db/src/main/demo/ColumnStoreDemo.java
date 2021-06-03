package main.demo;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.common.Constants;
import main.execution.ExecutorContext;
import main.execution.ExecutorFactory;
import main.execution.executors.AggregationExecutor;
import main.execution.executors.InsertExecutor;
import main.execution.expressions.*;
import main.execution.plans.AggregationPlanNode;
import main.execution.plans.AggregationType;
import main.execution.plans.InsertPlanNode;
import main.execution.plans.SeqReadPlanNode;
import main.storage.disk.DiskManager;
import main.storage.page.TablePage;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.DEBUGGER;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ColumnStoreDemo
{
    private List<Column> cols;
    private Schema s;
    private String table;

    private DiskManager diskManager ;

    private BufferPoolManager bufferPoolManager;

    private Catalog catalog ;

    private List<List<Value>> values;

    private ExecutorContext context;

    private InsertPlanNode ipn;

    private InsertExecutor ex;

    private List<Column> outputCols;

    private ComparisonExpression ce ;

    private SeqReadPlanNode readPlanNode;

    private Schema outputSchema;

    private AggregationPlanNode apn;


    public ColumnStoreDemo(){
        //`user_id`, `username`, `first_name`, `last_name`, `gender`
        cols = new ArrayList<>(){{
            add(new Column("USER_ID", TypeID.INTEGER_TYPE ));
            add(new Column("USERNAME", TypeID.STRING_TYPE ));
            add(new Column("FIRST_NAME", TypeID.STRING_TYPE ));
            add(new Column("LAST_NAME", TypeID.STRING_TYPE ));
            add(new Column("GENDER", TypeID.STRING_TYPE ));
            add(new Column("AGE", TypeID.INTEGER_TYPE ));
        }};
        s = new Schema(cols);
        table = "USER_DETAILS";
        diskManager = new DiskManager("ColumnDemo.db");
        bufferPoolManager = new BufferPoolManager(10, diskManager);
        catalog = new Catalog(bufferPoolManager);

        values = new ArrayList<>();

        values.add( new ArrayList<>(){{add (new Value(1));	add (new Value( "rogers63"));	add (new Value( "david"));	add (new Value( "john"));	add (new Value( "Female"));add ( new Value( 20 )); }});
        values.add( new ArrayList<>(){{add (new Value(2));	add (new Value( "mike28"));	add (new Value( "rogers"));	add (new Value( "paul"));	add (new Value( "Male"));add ( new Value(  22)); }});
        values.add( new ArrayList<>(){{add (new Value(3));	add (new Value( "rivera92"));	add (new Value( "david"));	add (new Value( "john"));	add (new Value( "Male"));add ( new Value(34  )); }});
        values.add( new ArrayList<>(){{add (new Value(4));	add (new Value( "ross95"));	add (new Value( "maria"));	add (new Value( "sanders"));	add (new Value( "Male"));add ( new Value( 18 )); }});
        values.add( new ArrayList<>(){{add (new Value(5));	add (new Value( "paul85"));	add (new Value( "morris"));	add (new Value( "miller"));	add (new Value( "Female"));add ( new Value( 37 )); }});
        values.add( new ArrayList<>(){{add (new Value(6));	add (new Value( "smith34"));	add (new Value( "daniel"));	add (new Value( "michael"));	add (new Value( "Female"));add ( new Value( 27 )); }});
        values.add( new ArrayList<>(){{add (new Value(7));	add (new Value( "james84"));	add (new Value( "sanders"));	add (new Value( "paul"));	add (new Value( "Female"));add ( new Value( 28 )); }});
        values.add( new ArrayList<>(){{add (new Value(8));	add (new Value( "daniel53"));	add (new Value( "mark"));	add (new Value( "mike"));	add (new Value( "Male"));add ( new Value( 22 )); }});
        values.add( new ArrayList<>(){{add (new Value(9));	add (new Value( "brooks80"));	add (new Value( "morgan"));	add (new Value( "maria"));	add (new Value( "Female"));add ( new Value( 25 )); }});
        values.add( new ArrayList<>(){{add (new Value(10));	add (new Value( "morgan65"));	add (new Value( "paul"));	add (new Value( "miller"));	add (new Value( "Female"));add ( new Value( 39 )); }});
        values.add( new ArrayList<>(){{add (new Value(11));	add (new Value( "sanders84"));	add (new Value( "david"));	add (new Value( "miller"));	add (new Value( "Female"));add ( new Value( 29 )); }});
        values.add( new ArrayList<>(){{add (new Value(12));	add (new Value( "maria40"));	add (new Value( "chrishaydon"));	add (new Value( "bell"));	add (new Value( "Female"));add ( new Value( 23 )); }});
        values.add( new ArrayList<>(){{add (new Value(13));	add (new Value( "brown71"));	add (new Value( "michael"));	add (new Value( "brown"));	add (new Value( "Male"));add ( new Value( 22)); }});
        values.add( new ArrayList<>(){{add (new Value(14));	add (new Value( "james63"));	add (new Value( "morgan"));	add (new Value( "james"));	add (new Value( "Male"));add ( new Value( 28 )); }});
        values.add( new ArrayList<>(){{add (new Value(15));	add (new Value( "jenny0993"));	add (new Value( "rogers"));	add (new Value( "chrishaydon"));	add (new Value( "Male"));add ( new Value( 25 )); }});
        values.add( new ArrayList<>(){{add (new Value(16));	add (new Value( "john96"));	add (new Value( "morgan"));	add (new Value( "wright"));	add (new Value( "Male"));add ( new Value( 23 )); }});
        values.add( new ArrayList<>(){{add (new Value(17));	add (new Value( "miller64"));	add (new Value( "morgan"));	add (new Value( "wright"));	add (new Value( "Male"));add ( new Value( 33 )); }});
        values.add( new ArrayList<>(){{add (new Value(18));	add (new Value( "mark46"));	add (new Value( "david"));	add (new Value( "ross"));	add (new Value( "Female"));add ( new Value( 24 )); }});
        values.add( new ArrayList<>(){{add (new Value(19));	add (new Value( "jenny0988"));	add (new Value( "maria"));	add (new Value( "morgan"));	add (new Value( "Female"));add ( new Value( 27 )); }});
        values.add( new ArrayList<>(){{add (new Value(20));	add (new Value( "mark80"));	add (new Value( "mike"));	add (new Value( "bell"));	add (new Value( "Male"));add ( new Value( 26 )); }});
        values.add( new ArrayList<>(){{add (new Value(21));	add (new Value( "morris72"));	add (new Value( "miller"));	add (new Value( "michael"));	add (new Value( "Male"));add ( new Value( 25 )); }});
        values.add( new ArrayList<>(){{add (new Value(22));	add (new Value( "wright39"));	add (new Value( "ross"));	add (new Value( "rogers"));	add (new Value( "Female"));add ( new Value( 20 )); }});
        values.add( new ArrayList<>(){{add (new Value(23));	add (new Value( "paul68"));	add (new Value( "brooks"));	add (new Value( "mike"));	add (new Value( "Male"));add ( new Value( 22 )); }});
        values.add( new ArrayList<>(){{add (new Value(24));	add (new Value( "smith60"));	add (new Value( "miller"));	add (new Value( "daniel"));	add (new Value( "Male"));add ( new Value( 27 )); }});
        values.add( new ArrayList<>(){{add (new Value(25));	add (new Value( "bell43"));	add (new Value( "mike"));	add (new Value( "wright"));	add (new Value( "Male"));add ( new Value( 24 )); }});
        values.add( new ArrayList<>(){{add (new Value(26));	add (new Value( "rogers79"));	add (new Value( "wright"));	add (new Value( "smith"));	add (new Value( "Female"));add ( new Value( 26 )); }});
        values.add( new ArrayList<>(){{add (new Value(27));	add (new Value( "daniel56"));	add (new Value( "david"));	add (new Value( "morgan"));	add (new Value( "Male"));add ( new Value( 20 )); }});
        values.add( new ArrayList<>(){{add (new Value(28));	add (new Value( "brooks85"));	add (new Value( "smith"));	add (new Value( "bell"));	add (new Value( "Female"));add ( new Value( 19 )); }});
        values.add( new ArrayList<>(){{add (new Value(29));	add (new Value( "mike30"));	add (new Value( "paul"));	add (new Value( "wright"));	add (new Value( "Female"));add ( new Value( 18 )); }});
        values.add( new ArrayList<>(){{add (new Value(30));	add (new Value( "paul92"));	add (new Value( "michael"));	add (new Value( "james"));	add (new Value( "Female"));add ( new Value( 21 )); }});
        values.add( new ArrayList<>(){{add (new Value(31));	add (new Value( "bell96"));	add (new Value( "michael"));	add (new Value( "sanders"));	add (new Value( "Female"));add ( new Value( 22 )); }});
        values.add( new ArrayList<>(){{add (new Value(32));	add (new Value( "john8"));	add (new Value( "john"));	add (new Value( "rivera"));	add (new Value( "Female"));add ( new Value( 28 )); }});
        values.add( new ArrayList<>(){{add (new Value(33));	add (new Value( "chrishaydon12"));	add (new Value( "paul"));	add (new Value( "michael"));	add (new Value( "Male"));add ( new Value( 29 )); }});
        values.add( new ArrayList<>(){{add (new Value(34));	add (new Value( "morgan13"));	add (new Value( "ross"));	add (new Value( "mark"));	add (new Value( "Female"));add ( new Value(25  )); }});
        values.add( new ArrayList<>(){{add (new Value(35));	add (new Value( "james83"));	add (new Value( "brooks"));	add (new Value( "smith"));	add (new Value( "Female"));add ( new Value( 39 )); }});
        values.add( new ArrayList<>(){{add (new Value(36));	add (new Value( "chrishaydon8"));	add (new Value( "cooper"));	add (new Value( "brown"));	add (new Value( "Female"));add ( new Value( 40 )); }});
        values.add( new ArrayList<>(){{add (new Value(37));	add (new Value( "ross85"));	add (new Value( "ross"));	add (new Value( "daniel"));	add (new Value( "Male"));add ( new Value(41  )); }});
        values.add( new ArrayList<>(){{add (new Value(38));	add (new Value( "ross46"));	add (new Value( "cooper"));	add (new Value( "miller"));	add (new Value( "Male"));add ( new Value( 20 )); }});
        values.add( new ArrayList<>(){{add (new Value(39));	add (new Value( "smith4"));	add (new Value( "jenny09"));	add (new Value( "maria"));	add (new Value( "Female"));add ( new Value( 27 )); }});
        values.add( new ArrayList<>(){{add (new Value(40));	add (new Value( "paul4"));	add (new Value( "paul"));	add (new Value( "rivera"));	add (new Value( "Female"));add ( new Value( 33 )); }});
        values.add( new ArrayList<>(){{add (new Value(41));	add (new Value( "daniel26"));	add (new Value( "maria"));	add (new Value( "sanders"));	add (new Value( "Male"));add ( new Value( 30 )); }});
        values.add( new ArrayList<>(){{add (new Value(42));	add (new Value( "chrishaydon2"));	add (new Value( "bell"));	add (new Value( "david"));	add (new Value( "Female"));add ( new Value( 31 )); }});
        values.add( new ArrayList<>(){{add (new Value(43));	add (new Value( "david82"));	add (new Value( "rivera"));	add (new Value( "cooper"));	add (new Value( "Male"));add ( new Value( 33 )); }});
        values.add( new ArrayList<>(){{add (new Value(44));	add (new Value( "john97"));	add (new Value( "mark"));	add (new Value( "david"));	add (new Value( "Female"));add ( new Value( 30 )); }});
        values.add( new ArrayList<>(){{add (new Value(45));	add (new Value( "david57"));	add (new Value( "paul"));	add (new Value( "cooper"));	add (new Value( "Male"));add ( new Value( 38 )); }});
        values.add( new ArrayList<>(){{add (new Value(46));	add (new Value( "rivera100"));	add (new Value( "brooks"));	add (new Value( "david"));	add (new Value( "Male"));add ( new Value(20  )); }});
        values.add( new ArrayList<>(){{add (new Value(47));	add (new Value( "bell13"));	add (new Value( "james"));	add (new Value( "maria"));	add (new Value( "Male"));add ( new Value( 19 )); }});
        values.add( new ArrayList<>(){{add (new Value(48));	add (new Value( "brooks65"));	add (new Value( "john"));	add (new Value( "mark"));	add (new Value( "Female"));add ( new Value( 18 )); }});
        values.add( new ArrayList<>(){{add (new Value(49));	add (new Value( "daniel40"));	add (new Value( "rivera"));	add (new Value( "jenny09"));	add (new Value( "Female"));add ( new Value( 19 )); }});
        values.add( new ArrayList<>(){{add (new Value(50));	add (new Value( "cooper100"));	add (new Value( "chrishaydon"));	add (new Value( "sanders"));	add (new Value( "Female"));add ( new Value( 35 )); }});

        context = new ExecutorContext(catalog, bufferPoolManager);
        catalog.createTable(table, s);
        ipn = new InsertPlanNode(values, catalog.getTable(table).getOid());
        ex = (InsertExecutor) ExecutorFactory.createExecutor(context, ipn, 1);

        TablePage insertCol;
        insertCol = ex.insertColStore(values, catalog, table);
        ex.insertColVals(values, catalog, table);



        outputCols = new ArrayList<>(){{
            add(new Column("Gender",
                    TypeID.STRING_TYPE,
                    new AggregateValueExpression(true,0, TypeID.STRING_TYPE) ));
            add(new Column("Users < 25",
                    TypeID.INTEGER_TYPE,
                    new AggregateValueExpression(false,0, TypeID.INTEGER_TYPE) ));
        }};

        ce = new ComparisonExpression(
                new ColumnValueExpression(-1,5, TypeID.INTEGER_TYPE),
                new ConstantValueExpression(new Value(25)),
                ComparisonType.LessThanOrEqual);
        readPlanNode = new SeqReadPlanNode(s, ce, catalog.getTable(table).getOid());

        outputSchema = new Schema(outputCols);
        apn = new AggregationPlanNode(outputSchema,
                readPlanNode,
                null,
                new ArrayList<>(){{
                    add(new ColumnValueExpression(-1,4, TypeID.STRING_TYPE) );
                }},
                new ArrayList<>(){{
                    add(new ColumnValueExpression(-1,5, TypeID.INTEGER_TYPE) );
                }},
                new ArrayList<>(){{
                    add(AggregationType.CountAggregate);
                }});

        AggregationExecutor aex = (AggregationExecutor) ExecutorFactory.createExecutor(context, apn, 1);
        DEBUGGER.info("Running following query: SELECT GENDER, COUNT(*) FROM USERS WHERE USERS.AGE < 25 GROUP BY GENDER ");
        aex.computeAggregate(values, 4, outputSchema, 25);
    }

}
