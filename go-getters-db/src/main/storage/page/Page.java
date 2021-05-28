package main.storage.page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import static main.common.Constants.PAGE_SIZE;

public class Page<T> implements Serializable
{
    private int count;
    private List<T> data;

    private int pageID = -1;
    private int nextPageID = 0;

    public int pinCount = 0;
    public boolean isDirty = false;

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

    public Page(T page) {
        this();
        this.data.add(page);
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
        this.nextPageID = this.pageID + 1;
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
}