package main.storage.page;

        import java.util.ArrayList;
        import java.util.List;
        import java.util.concurrent.locks.Lock;
        import java.util.concurrent.locks.ReadWriteLock;
        import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Page<T> {

    private int count;
    private List<T> data;

    private int pageID;
    private int nextPageID;

    public int pinCount;
    public boolean isDirty;

    private final ReadWriteLock rwlatch
            = new ReentrantReadWriteLock();
    private final Lock writeLock
            = rwlatch.writeLock();
    private final Lock readLock = rwlatch.readLock();


    public void resetMemory(){
        data = new ArrayList<T>();
    }

    public Page() {
        resetMemory();
    }

    public List<T> getData() {
        return data;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getCount() {
        return count;
    }

    public int getPageID() {
        return pageID;
    }

    public void setPageID(int pageID) {
        this.pageID  = pageID;
    }

    public int getPinCount() {
        return pinCount;
    }

    public void wUnlatch() {
        writeLock.unlock();
    }

    public void wLatch() {
        writeLock.lock();
    }

    public void rUnlatch() {
        readLock.unlock();
    }

    public void rLatch() {
        readLock.lock();
    }

    public int getNextPageID() {
        return nextPageID;
    }

    public void setNextPageID(int nextPageID) {
        this.nextPageID = nextPageID;
    }

    public String getStringData()
    {
        StringBuilder stringData = new StringBuilder();

        for (int i = 0; i < PAGE_SIZE; i++)
        {
            stringData.append(data.get(i).getClass().toString());
        }
        return stringData.toString();
    }
}