package test.buffer;
import main.buffer.BufferPoolManager;
import main.storage.disk.DiskManager;
import main.storage.page.Page;
import static org.junit.jupiter.api.Assertions.*;

public class BufferPoolManagerTest
{
    @org.junit.jupiter.api.Test
    void BufferPoolManagerTest1()
    {
        int tempPageID = 0;

        DiskManager<Page<String>> diskManager = new DiskManager<>("BufferPoolDBTest1");

        BufferPoolManager<String> bufferPoolManager = new BufferPoolManager<>(10, diskManager);

        Page<String> pageZero = bufferPoolManager.newPage(tempPageID);

        assertNotNull(pageZero);
        assertEquals(0, tempPageID);
        pageZero.getData().add("Hello");


        for (int i = 1; i < 10; ++i) {
            assertNotNull(bufferPoolManager.newPage(tempPageID));
        }

        // all the pages are pinned, the buffer pool is full
        for (int i = 1; i < 15; ++i) {
            assertNull(bufferPoolManager.newPage(tempPageID));
        }

        // unpin the first five pages, add them to LRU list, set as dirty
        for (int i = 0; i < 5; ++i) {
            assertTrue(bufferPoolManager.unpinPage(i, true));
        }

        // we have 5 empty slots in LRU list, evict page zero out of buffer pool
        for (int i = 10; i < 14; ++i) {
            assertNotNull(bufferPoolManager.newPage(tempPageID));
        }

        // fetch page one again
        Page<String> pageRead = diskManager.readPage(0, new Page<>());
        pageZero = bufferPoolManager.fetchPage(0);

        // check read content
        assertEquals(pageRead.getData(), pageZero.getData());

        diskManager.shutDown();
    }

    @org.junit.jupiter.api.Test
    void BufferPoolManagerTest2()
    {
        int tempPageID = 0;

        DiskManager<Page<String>> diskManager = new DiskManager<>("BufferPoolDBTest2");

        BufferPoolManager<String> bufferPoolManager = new BufferPoolManager<>(10, diskManager);

        Page<String> pageZero = bufferPoolManager.newPage(tempPageID);

        assertNotNull(pageZero);
        assertEquals(0, tempPageID);
        pageZero.getData().add("Hello");


        for (int i = 1; i < 10; ++i) {
            assertNotEquals(null, bufferPoolManager.newPage(tempPageID));
        }

        // upin the first five pages, add them to LRU list, set as dirty
        for (int i = 0; i < 1; ++i)
        {
            assertTrue(bufferPoolManager.unpinPage(i, true));
            pageZero = bufferPoolManager.fetchPage(0);
            assertEquals(pageZero.getData().get(0), "Hello");
            assertTrue(bufferPoolManager.unpinPage(i, true));
            assertNotNull(bufferPoolManager.newPage(tempPageID));
        }

        int[] test = {5, 6, 7, 8, 9, 10};

        for (int i: test)
        {
            Page<String> page = bufferPoolManager.fetchPage(i);

            assert page != null;

            assertEquals(i, page.getPageID());

            bufferPoolManager.unpinPage(i, true);
        }

        bufferPoolManager.unpinPage(10, true);

        // fetch page one again
        Page<String> pageRead = diskManager.readPage(0, new Page<>());
        pageZero = bufferPoolManager.fetchPage(0);

        // check read content
        assertEquals(pageRead.getData(), pageZero.getData());

        diskManager.shutDown();
    }
}
