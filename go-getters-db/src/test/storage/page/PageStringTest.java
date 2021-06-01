package test.storage;

import main.catalog.Column;
import main.catalog.Schema;
import main.storage.page.TablePage;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.*;
import static org.junit.Assert.assertEquals;

public class PageStringTest {

    @org.junit.jupiter.api.Test
    void tablePageTest() {

        List<Column> cols = new ArrayList<>(){{
           add(new Column("ID", TypeID.INTEGER_TYPE ));
           add(new Column("Name", TypeID.STRING_TYPE ));
           add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);

        TablePage tp = new TablePage();
        tp.setCount(4);
        tp.setDirty(false);
        tp.setNextPageID(INVALID_PAGE_ID);
        tp.setPageID(513);
        tp.setPinCount(4);
        tp.setSchema(s);

        List<Tuple> values = tp.getData();
        values.add(new Tuple(new ArrayList<>(){{
            add(new Value(1));
            add(new Value("ABC"));
            add(new Value(true));
        }}));

        values.add(new Tuple(new ArrayList<>(){{
            add(new Value(2));
            add(new Value("BCD"));
            add(new Value(true));
        }}));

        values.add(new Tuple(new ArrayList<>(){{
            add(new Value(3));
            add(new Value("CDE"));
            add(new Value(true));
        }}));

        values.add(new Tuple(new ArrayList<>(){{
            add(new Value(4));
            add(new Value("DEF"));
            add(new Value(false));
        }}));

        String rep = tp.toString();

        String expected = "4"+LINE_SEP+"false"+LINE_SEP+"-1"+LINE_SEP+"513"+LINE_SEP+"4"+LINE_SEP
                        + "1"+COLUMN_SEP+"ABC"+COLUMN_SEP+"true"+LINE_SEP
                        + "2"+COLUMN_SEP+"BCD"+COLUMN_SEP+"true"+LINE_SEP
                        + "3"+COLUMN_SEP+"CDE"+COLUMN_SEP+"true"+LINE_SEP
                        + "4"+COLUMN_SEP+"DEF"+COLUMN_SEP+"false";

        assertEquals(expected, rep);

        TablePage fresh = new TablePage();
        fresh.setSchema(s);
        fresh.initializePageFromString(rep);
        assertEquals(fresh.getCount(), 4);
        assertEquals(fresh.isDirty, false);
        assertEquals(fresh.getNextPageID(), -1);
        assertEquals(fresh.getPageID(), 513);
        assertEquals(fresh.getPinCount(), 4);

        Schema freshSchema = fresh.getSchema();

        assertEquals(freshSchema.getColumn(0).getName(), "ID");
        assertEquals(freshSchema.getColumn(0).getType(), TypeID.INTEGER_TYPE);
        assertEquals(freshSchema.getColumn(1).getName(), "Name");
        assertEquals(freshSchema.getColumn(1).getType(), TypeID.STRING_TYPE);
        assertEquals(freshSchema.getColumn(2).getName(), "IsAdult");
        assertEquals(freshSchema.getColumn(2).getType(), TypeID.BOOLEAN_TYPE);

        List<Tuple> freshTuples = fresh.getData();
        assertEquals(freshTuples.get(0).getValue(0), new Value(1));
        assertEquals(freshTuples.get(0).getValue(1), new Value("ABC"));
        assertEquals(freshTuples.get(0).getValue(2), new Value(true));

        assertEquals(freshTuples.get(1).getValue(0), new Value(2));
        assertEquals(freshTuples.get(1).getValue(1), new Value("BCD"));
        assertEquals(freshTuples.get(1).getValue(2), new Value(true));

        assertEquals(freshTuples.get(2).getValue(0), new Value(3));
        assertEquals(freshTuples.get(2).getValue(1), new Value("CDE"));
        assertEquals(freshTuples.get(2).getValue(2), new Value(true));

        assertEquals(freshTuples.get(3).getValue(0), new Value(4));
        assertEquals(freshTuples.get(3).getValue(1), new Value("DEF"));
        assertEquals(freshTuples.get(3).getValue(2), new Value(false));

    }

    @org.junit.jupiter.api.Test
    void emptyPageTest() {

        List<Column> cols = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);

        TablePage tp = new TablePage();
        tp.setCount(4);
        tp.setDirty(false);
        tp.setNextPageID(INVALID_PAGE_ID);
        tp.setPageID(513);
        tp.setPinCount(4);
        tp.setSchema(s);

        String rep = tp.toString();

        String expected = "4"+LINE_SEP+"false"+LINE_SEP+"-1"+LINE_SEP+"513"+LINE_SEP+"4";

        assertEquals(expected, rep);

        TablePage fresh = new TablePage();
        fresh.setSchema(s);
        fresh.initializePageFromString(rep);
        assertEquals(fresh.getCount(), 4);
        assertEquals(fresh.isDirty, false);
        assertEquals(fresh.getNextPageID(), -1);
        assertEquals(fresh.getPageID(), 513);
        assertEquals(fresh.getPinCount(), 4);

        Schema freshSchema = fresh.getSchema();


    }

}
