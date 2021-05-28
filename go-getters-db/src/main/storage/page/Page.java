package main.storage.page;

import main.common.StringInitializable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static main.common.Constants.INVALID_PAGE_ID;
import static main.common.Constants.LINE_SEP;

public abstract class Page {

    private int count;
    private int pageID = INVALID_PAGE_ID;
    private int nextPageID = INVALID_PAGE_ID;

    public int pinCount = 0 ;
    public boolean isDirty = false;

    private final ReadWriteLock rwlatch
            = new ReentrantReadWriteLock();
    private final Lock writeLock
            = rwlatch.writeLock();
    private final Lock readLock = rwlatch.readLock();


    public Page() {
    }

    public abstract  void resetMemory();

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

    @Override
    public String toString() {
        StringBuilder res = new StringBuilder();
        res.append(count);
        res.append(LINE_SEP);
        res.append(isDirty);
        res.append(LINE_SEP);
        res.append(nextPageID);
        res.append(LINE_SEP);
        res.append(pageID);
        res.append(LINE_SEP);
        res.append(pinCount);
        res.append(LINE_SEP);

        return res.toString();
    }

    public void initializePageFromString(String input) {

        String[] parts = input.split(String.valueOf(LINE_SEP));

        count = Integer.valueOf(parts[0]);
        isDirty = Boolean.valueOf(parts[1]);
        nextPageID = Integer.valueOf(parts[2]);
        pageID = Integer.valueOf(parts[3]);
        pinCount = Integer.valueOf(parts[4]);

        return;
    }
}