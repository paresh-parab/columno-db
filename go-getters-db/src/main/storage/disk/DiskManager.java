package main.storage.disk;

import main.storage.page.Page;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static main.common.Constants.PAGE_SIZE;


public class DiskManager
{
    private int numFlushes = 0, numWrites = 0;
    private boolean flushLog = false;

    File logIO, dbIO;
    String logName;
    static String bufferUsed;

    AtomicInteger nextPageID = new AtomicInteger(); //Maybe needs initialization
    Future<Void> flushLogF;

    DiskManager(String dbFile)
    {
        int n = dbFile.lastIndexOf('.');

        if (n == dbFile.length()) { return; }

        String logName = dbFile.substring(0, n) + ".log";

        try
        {
            logIO = new File(logName);
            boolean flag = logIO.createNewFile();

            if (flag)
            {
                System.out.println("Log File created successfully");
                // LOG_DEBUG("Log File created successfully");
            }
            else
            {
                System.out.println("Log File already present");
                // LOG_DEBUG("Log File already present");
            }
        }
        catch (IOException e)
        {
            System.out.println("Cannot open log file");
            // LOG_DEBUG("Cannot open log file");
            e.printStackTrace();
        }

        try
        {
            dbIO = new File(dbFile);
            boolean flag = dbIO.createNewFile();

            if (flag)
            {
                System.out.println("DB File created successfully");
                // LOG_DEBUG("DB File created successfully");
            }
            else
            {
                System.out.println("DB File already present");
                // LOG_DEBUG("DB File already present");
            }
        }
        catch (IOException e)
        {
            System.out.println("Cannot open DB file");
            // LOG_DEBUG("Cannot open DB file");
            e.printStackTrace();
        }
    }

    public void writePage(int pageID, List<Page> pageData)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites += 1;

        try
        {
            RandomAccessFile rap = new RandomAccessFile(dbIO.getName(), "rwd");
            rap.seek(offset);
            rap.writeUTF(String.valueOf(pageData));
            rap.getFD().sync();
        }
        catch (IOException e)
        {
            System.out.println("Unable to write to page");
            // LOG_DEBUG("Unable to write to page");
            e.printStackTrace();
        }
    }

    public void readPage(int pageID, List<Page> pageData)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites = getNumWrites();
        numWrites += 1;

        if (offset > dbIO.length())
        {
            // LOG_DEBUG("I/O error reading past end of file");
            System.out.println("I/O error reading past end of file");
        }
        else
        {
            try
            {
                RandomAccessFile rap = new RandomAccessFile(dbIO.getName(), "rwd");
                rap.seek(offset);
                rap.readUTF();
            }
            catch (IOException e)
            {
                System.out.println("I/O error while reading");
                // LOG_DEBUG("I/O error while reading");
                e.printStackTrace();
            }
        }
    }

    boolean readLog(String logData, int size, int offset)
    {
        if (offset >= logName.length()) return false;

        try
        {
            RandomAccessFile rap = new RandomAccessFile(logIO.getName(), "rwd");
            rap.seek(offset);
            rap.readUTF();
        }
        catch (IOException e)
        {
            System.out.println("I/O error while reading log");
            // LOG_DEBUG("I/O error while reading log");
            e.printStackTrace();
            return false;
        }

        return true;
    }

    void writeLog(String logData, int size)
    {
        try
        {
            assert(!logData.equals(bufferUsed)); bufferUsed = logData;

            if (size == 0) return;

            flushLog = getFlushState(); flushLog = true;

            if (!hasFlushLogFuture())
            {
                flushLogF.get(1000, TimeUnit.SECONDS);
                setFlushLogFuture(flushLogF);
            }

            numFlushes = getNumFlushes();
            numFlushes += 1;

                RandomAccessFile rap = new RandomAccessFile(logIO.getName(), "rwd");
                rap.writeUTF(logData);
                rap.getFD().sync();
                flushLog = false;
        }
        catch (Exception e)
        {
            System.out.println("I/O error while writing log");
            e.printStackTrace();
            // LOG_DEBUG("I/O error while writing log");
        }
    }

    public int allocatePage() { return nextPageID.incrementAndGet(); }

    public void deallocatePage(int pageID) {}

    int getNumFlushes() { return numFlushes; }

    int getNumWrites() { return numWrites; }

    boolean getFlushState() { return flushLog; }

    void setFlushLogFuture(Future<Void> flushLogFuture) { flushLogF = flushLogFuture; }

    boolean hasFlushLogFuture() { return flushLogF == null; }

}
