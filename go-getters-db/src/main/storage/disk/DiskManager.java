package main.storage.disk;
import java.io.*;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.System.exit;
import static main.common.Constants.PAGE_SIZE;


public class DiskManager<T>
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
        this.logName = dbFile + ".log";

        dbIO = new File(dbFile);

        boolean flag = dbIO.mkdirs();

        if (flag) {
            System.out.println("DB created successfully");
        }
        else {
            System.out.println("DB already present");
        }

        try
        {
            logIO = new File("./" + dbIO + "/" + logName);
            flag = logIO.createNewFile();

            if (flag) {
                System.out.println("Log File created successfully");
            }
            else {
                System.out.println("Log File already present");
            }
        }
        catch (IOException e) {
            System.out.println("Exception: Cannot open log file");
            e.printStackTrace();
            exit(1);
        }
    }

    public void writePage(int pageID, T pageData)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites += 1;

        try
        {
            ObjectOutputStream oos = new ObjectOutputStream(
                    new FileOutputStream(dbIO.getName() + "/" + pageID));
            oos.writeObject(pageData);
            oos.flush();
            oos.close();
        }
        catch (IOException e)
        {
            System.out.println("Exception: Unable to write to page");
            e.printStackTrace();
            exit(1);
        }
    }

    public T readPage(int pageID, T pageData)
    {
        int offset = pageID * PAGE_SIZE;
        numWrites = getNumWrites();
        numWrites += 1;

        if (offset > dbIO.length()) {
            System.out.println("Exception: I/O error reading past end of file");
        }
        else
        {
            try
            {
                ObjectInputStream ois = new ObjectInputStream(
                        new FileInputStream(dbIO.getName() + "/" + pageID));
                pageData = (T)ois.readObject();
                ois.close();
            }
            catch (IOException | ClassNotFoundException e)
            {
                System.out.println("Exception: I/O error while reading");
                e.printStackTrace();
                return null;
            }
        }
        return pageData;
    }

    public String readLog(int offset)
    {
        if (offset >= logName.length()) return null;

        String logData;
        try
        {
            RandomAccessFile rap = new RandomAccessFile(
                    dbIO.getName() + "/" + logIO.getName(), "rwd");
            rap.seek(offset);
            logData = rap.readUTF();
        }
        catch (IOException e)
        {
            System.out.println("I/O error while reading log");
            e.printStackTrace();
            return null;
        }
        return logData;
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

                RandomAccessFile rap = new RandomAccessFile(
                        dbIO.getName() + "/" + logIO.getName(), "rwd");
                rap.writeUTF(logData);
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

    public void deallocatePage(int pageID) { delete(new File(dbIO.getName() + "/" + pageID));}

    int getNumFlushes() { return numFlushes; }

    int getNumWrites() { return numWrites; }

    boolean getFlushState() { return flushLog; }

    void setFlushLogFuture(Future<Void> flushLogFuture) { flushLogF = flushLogFuture; }

    boolean hasFlushLogFuture() { return flushLogF == null; }

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
    }
    public void shutDown()
    {
        delete(this.dbIO);
        System.out.println("DB and Log file deleted successfully");
    }
}
