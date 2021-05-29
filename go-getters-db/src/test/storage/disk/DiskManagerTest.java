package test.storage.disk;

import main.storage.disk.DiskManager;
import org.junit.jupiter.api.Assertions;

public class DiskManagerTest
{
    @org.junit.jupiter.api.Test
    void DiskManagerTest1()
    {
        String buffer = null;
        String tempData = "A test String.";

        String dbFile = "Sample.db";

        DiskManager diskManager = new DiskManager(dbFile);

        Assertions.assertNull(diskManager.readPage(0)); // tolerate empty read

        diskManager.writePage(0, tempData);

        buffer = diskManager.readPage(0);

        if(buffer.equals(tempData)) assert true;

        buffer = null;

        diskManager.writePage(5, tempData);
        buffer = diskManager.readPage(5);

        if(buffer.equals(tempData)) assert true;

        diskManager.shutDown();
    }

    @org.junit.jupiter.api.Test
    void DiskManagerTest2()
    {
        String buffer = null;

        String dbFile = "Test.db";

        DiskManager diskManager = new DiskManager(dbFile);
        String tempData = "A test string.";

        Assertions.assertNull(diskManager.readLog(0)); // tolerate empty read

        diskManager.writeLog(tempData);
        buffer = diskManager.readLog(0);

        if(buffer.equals(tempData)) assert true;

        diskManager.shutDown();
    }

}
