package test.storage.disk;

import main.storage.disk.DiskManager;
import main.storage.page.Page;
import org.junit.jupiter.api.Assertions;

public class DiskManagerTest
{
    @org.junit.jupiter.api.Test
    void DiskManagerTest1()
    {
        Page<String> buffer = null;

        String dbFile = "Sample_DB";

        DiskManager<Page<String>> diskManager = new DiskManager<>(dbFile);
        Page<String> tempPage = new Page<>();
        tempPage.getData().add("A test string.");

        Assertions.assertNull(diskManager.readPage(0, buffer)); // tolerate empty read

        diskManager.writePage(0, tempPage);
        buffer = diskManager.readPage(0, buffer);
        Assertions.assertEquals(buffer.getData(), tempPage.getData());

        buffer = null;

        diskManager.writePage(5, tempPage);
        buffer = diskManager.readPage(5, buffer);

        Assertions.assertEquals(buffer.getData(), tempPage.getData());
        diskManager.shutDown();
    }

    @org.junit.jupiter.api.Test
    void DiskManagerTest2()
    {
        String buffer = "";

        String dbFile = "test";

        DiskManager<String> diskManager = new DiskManager<>(dbFile);
        String data = "A test string.";

        Assertions.assertNull(diskManager.readLog(0)); // tolerate empty read

        diskManager.writeLog(data);
        buffer = diskManager.readLog(0);

        Assertions.assertEquals(buffer, data);
        diskManager.shutDown();
    }

}
