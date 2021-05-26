package test.storage.disk;

import main.storage.disk.DiskManager;

import java.util.Arrays;

import static main.common.Constants.PAGE_SIZE;
import static org.junit.jupiter.api.Assertions.*;

public class DiskManagerTest
{
    @org.junit.jupiter.api.Test
    void DiskManagerTest1()
    {

        char[] buffer = new char[PAGE_SIZE];
        char[] data = new char[PAGE_SIZE];

        String dbFile = "test.db";

        DiskManager<T> diskManager = new DiskManager<T>(dbFile);
        String testString = "A test string.";
        testString.getChars( 0, data.length, data , 0 );

        diskManager.readPage(0, buffer); // tolerate empty read

        diskManager.writePage(0, data);
        diskManager.readPage(0, buffer);

        assertArrayEquals(buffer, data);

        buffer = null;

        diskManager.writePage(5, data);
        diskManager.readPage(5, buffer);

        assertArrayEquals(buffer, data);
    }
}
