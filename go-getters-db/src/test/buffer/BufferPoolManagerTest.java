package test.buffer;
import main.buffer.BufferPoolManager;
import main.catalog.Column;
import main.catalog.Schema;
import main.storage.disk.DiskManager;
import main.storage.page.Page;
import main.storage.page.TablePage;
import main.storage.table.Tuple;
import main.type.PageType;
import main.type.TypeID;
import main.type.Value;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.INVALID_PAGE_ID;
import static org.junit.jupiter.api.Assertions.*;

public class BufferPoolManagerTest
{
    @org.junit.jupiter.api.Test
    void BufferPoolManagerTest1()
    {
        DiskManager diskManager = new DiskManager("BufferPoolDBTest1.db");

        BufferPoolManager bufferPoolManager = new
                BufferPoolManager(10, diskManager);

        Page pageZero = bufferPoolManager.newPage(PageType.TABLE);

        List<Column> cols = new ArrayList<>(){{
            add(new Column("ID", TypeID.INTEGER_TYPE ));
            add(new Column("Name", TypeID.STRING_TYPE ));
            add(new Column("IsAdult", TypeID.BOOLEAN_TYPE ));
        }};
        Schema s = new Schema(cols);

        TablePage tp = new TablePage();
        tp.setCount(4);
        tp.setNextPageID(INVALID_PAGE_ID);
        tp.setPageID(513);
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

        System.out.println(tp.toString());

        pageZero = tp;

        assertNotNull(pageZero);


        for (int i = 1; i < 10; ++i) {
            assertNotNull(bufferPoolManager.newPage(PageType.TABLE));
        }

        // all the pages are pinned, the buffer pool is full
        for (int i = 1; i < 15; ++i) {
            assertNull(bufferPoolManager.newPage(PageType.TABLE));
        }

        // unpin the first five pages, add them to LRU list, set as dirty
        for (int i = 0; i < 5; ++i) {
            assertTrue(bufferPoolManager.unpinPage(i, true));
        }

        // we have 5 empty slots in LRU list, evict page zero out of buffer pool
        for (int i = 10; i < 14; ++i) {
            assertNotNull(bufferPoolManager.newPage(PageType.TABLE));
        }

        // fetch page one again
        Page pageRead = new TablePage();
        pageRead.initializePageFromString(diskManager.readPage(0));
        pageZero = bufferPoolManager.fetchPage(0);

        // check read content
        assertEquals(pageRead.getPageID(), pageZero.getPageID());

        diskManager.shutDown();
    }

    @org.junit.jupiter.api.Test
    void BufferPoolManagerTest2()
    {
//        int tempPageID = 0;
//
//        DiskManager diskManager = new DiskManager("BufferPoolDBTest2");
//
//        BufferPoolManager bufferPoolManager = new BufferPoolManager(10, diskManager);
//
//        Page pageZero = bufferPoolManager.newPage(PageType.TABLE);
//
//        assertNotNull(pageZero);
//        assertEquals(0, tempPageID);
//        pageZero.getData().add("Hello");
//
//
//        for (int i = 1; i < 10; ++i) {
//            assertNotEquals(null, bufferPoolManager.newPage(tempPageID));
//        }
//
//        // upin the first five pages, add them to LRU list, set as dirty
//        for (int i = 0; i < 1; ++i)
//        {
//            assertTrue(bufferPoolManager.unpinPage(i, true));
//            pageZero = bufferPoolManager.fetchPage(0);
//            assertEquals(pageZero.getData().get(0), "Hello");
//            assertTrue(bufferPoolManager.unpinPage(i, true));
//            assertNotNull(bufferPoolManager.newPage(tempPageID));
//        }
//
//        int[] test = {5, 6, 7, 8, 9, 10};
//
//        for (int i: test)
//        {
//            Page<String> page = bufferPoolManager.fetchPage(i);
//
//            assert page != null;
//
//            assertEquals(i, page.getPageID());
//
//            bufferPoolManager.unpinPage(i, true);
//        }
//
//        bufferPoolManager.unpinPage(10, true);
//
//        // fetch page one again
//        Page<String> pageRead = diskManager.readPage(0, new Page<>());
//        pageZero = bufferPoolManager.fetchPage(0);
//
//        // check read content
//        assertEquals(pageRead.getData(), pageZero.getData());
//
//        diskManager.shutDown();
    }
}
