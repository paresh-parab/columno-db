package main.storage.disk;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.exit;
import static java.nio.file.Files.delete;
import static main.common.Constants.PAGE_SIZE;


public class DiskManager
{
    private int numFlushes = 0, numWrites = 0;
    private boolean flushLog = false;

    File logIO, dbIO;
    String logName;
    static String bufferUsed;

    AtomicInteger nextPageID = new AtomicInteger(-1);
    Future<Void> flushLogF;

    public DiskManager(String dbFile)
    {
        int n = dbFile.lastIndexOf('.');

        if (n == dbFile.length()) { return; }

        String logName = dbFile.substring(0, n) + ".log";

        try
        {
            logIO = new File(logName);
            boolean flag = logIO.createNewFile();

            if (flag) {
                System.out.println("Log File created successfully");
            }
            else {
                System.out.println("Log File already present");
            }
        }
        catch (IOException e)
        {
            System.out.println("Cannot open log file");
            e.printStackTrace();
        }

        try
        {
            dbIO = new File(dbFile);
            boolean flag = dbIO.createNewFile();

            if (flag) {
                System.out.println("DB File created successfully");
            }
            else {
                System.out.println("DB File already present");
            }
        }
        catch (IOException e)
        {
            System.out.println("Cannot open DB file");
            e.printStackTrace();
        }
    }

    public void writePage(int pageID, String pageData)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites += 1;

        byte[] byteData = new byte[PAGE_SIZE];
        System.arraycopy(pageData.getBytes(StandardCharsets.UTF_8),
                0, byteData, PAGE_SIZE - pageData.length(), pageData.length());
        try
        {
            /*
            ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(dbIO.getName() + "/" + pageID));
            oos.writeObject(pageData);
            oos.flush();
            oos.close(); */

            RandomAccessFile rap = new RandomAccessFile(dbIO.getName(), "rwd");
            rap.seek(offset);
            rap.write(byteData);
            rap.getFD().sync();
            flushLog = false;
        }
        catch (IOException e)
        {
            System.out.println("Exception: Unable to write to page");
            e.printStackTrace();
            exit(1);
        }
    }

    public String readPage(int pageID)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites = getNumWrites();
        numWrites += 1;
        byte[] pageData = new byte[PAGE_SIZE];

        if (offset > dbIO.length()) {
            System.out.println("Exception: I/O error reading past end of file");
        }
        else
        {
            try
            {
                /*
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(dbIO.getName() + "/" + pageID));
                pageData = (T)ois.readObject();
                ois.close();
                 */
                RandomAccessFile rap = new RandomAccessFile(dbIO.getName(), "rwd");
                rap.seek(offset);
                rap.readFully(pageData);
            }
            catch (Exception e)
            {
                System.out.println("Exception: I/O error while reading");
                e.printStackTrace();
                return null;
            }
        }
        return new String(pageData, StandardCharsets.UTF_8);
    }

    public String readLog(int offset)
    {
        byte[] logData = new byte[PAGE_SIZE];
        try
        {
            RandomAccessFile rap = new RandomAccessFile(logIO.getName(), "rwd");
            rap.seek(offset);
            rap.readFully(logData);
        }
        catch (IOException e)
        {
            System.out.println("I/O error while reading log");
            e.printStackTrace();
            return null;
        }
        return new String(logData, StandardCharsets.UTF_8);
    }

    public void writeLog(String logData)
    {
        try
        {
            assert(!logData.equals(bufferUsed)); bufferUsed = logData;

            flushLog = getFlushState(); flushLog = true;

            if (!hasFlushLogFuture())
            {
                flushLogF.get(1000, TimeUnit.SECONDS);
                setFlushLogFuture(flushLogF);
            }

            numFlushes = getNumFlushes();
            numFlushes += 1;

            byte[] byteData = new byte[PAGE_SIZE];
            System.arraycopy(logData.getBytes(StandardCharsets.UTF_8),
                    0, byteData, PAGE_SIZE - logData.length(), logData.length());

            RandomAccessFile rap = new RandomAccessFile(logIO.getName(), "rwd");
            rap.write(byteData);
            rap.getFD().sync();
            flushLog = false;
        }
        catch (Exception e)
        {
            System.out.println("Exception: I/O error while writing log");
            e.printStackTrace();
            exit(1);
        }
    }

    public int allocatePage() { return nextPageID.incrementAndGet(); }

    public void deallocatePage(int pageID) { }

    int getNumFlushes() { return numFlushes; }

    int getNumWrites() { return numWrites; }

    boolean getFlushState() { return flushLog; }

    void setFlushLogFuture(Future<Void> flushLogFuture) { flushLogF = flushLogFuture; }

    boolean hasFlushLogFuture() { return flushLogF == null; }

    /*
    public void delete(File db)
    {
        try {
            if (db.isDirectory()) {
                for (File c : Objects.requireNonNull(db.listFiles()))
                    delete(c);
            }
        }
        catch (Exception e) {
            System.out.println("Failed to delete file: " + db);
            e.printStackTrace();
        }
    }*/
    public void shutDown()
    {
        try {
            delete(dbIO.toPath());
            delete(logIO.toPath());
            System.out.println("DB and Log file deleted successfully");
        }
        catch (Exception e)
        {
            System.out.println("Exception: Error in deleting the DB");
            e.printStackTrace();
            exit(1);
        }
    }
}
