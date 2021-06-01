package test.storage.storage.table;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;
import main.catalog.Column;
import main.catalog.Schema;
import main.common.Pair;
import main.storage.disk.DiskManager;
import main.storage.table.TableHeap;
import main.storage.table.Tuple;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;

public class TableHeapTest {

    @org.junit.jupiter.api.Test
    public void testTupleInsertion(){

        List<Column> cols = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);
        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager);

        Catalog catalog = new Catalog(bufferPoolManager);

        Catalog.TableMetadata tableMetadata = catalog.createTable("FirstTable", s);

        TableHeap heap = tableMetadata.getTable();

        heap.insertTuple(new Tuple(new ArrayList<>(){{
            add(new Value(1));
            add(new Value("ABC"));
            add(new Value(true));
        }}));

        heap.insertTuple(new Tuple(new ArrayList<>(){{
            add(new Value(2));
            add(new Value("BCD"));
            add(new Value(true));
        }}));

        heap.insertTuple(new Tuple(new ArrayList<>(){{
            add(new Value(3));
            add(new Value("CDE"));
            add(new Value(true));
        }}));

        List pairs = heap.getColumnValues(0, TypeID.INTEGER_TYPE);

        assertEquals(3, pairs.size());

        assertEquals((Integer)1, (Integer)((Pair<Integer, Integer>)pairs.get(0)).getKey());
        assertEquals((Integer)2, (Integer)((Pair<Integer, Integer>)pairs.get(1)).getKey());
        assertEquals((Integer)3, (Integer)((Pair<Integer, Integer>)pairs.get(2)).getKey());
    }


}
