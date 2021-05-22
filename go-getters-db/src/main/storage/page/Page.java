package main.storage.page;

import main.common.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static main.common.Constants.PAGE_SIZE;

public class Page<KeyType, ValueType> {

    private int count;
    private List<Pair<KeyType, ValueType>> data;
    private int pageID;
    public int pinCount;
    public boolean isDirty;

    private final ReadWriteLock rwlatch
            = new ReentrantReadWriteLock();
    private final Lock writeLock
            = rwlatch.writeLock();
    private final Lock readLock = rwlatch.readLock();

    public void resetMemory(){
        data = new ArrayList<>();
        for(int i=0; i< PAGE_SIZE; i++){
            data.add( new Pair<>()) ;
        }
    }

    public void addEntry(Pair p, int index){
        data.get(index).setKey((KeyType) p.getKey());
        data.get(index).setValue((ValueType) p.getValue());
    }

    public void removeEntry(int index){
        data.remove(index);
        data.add(new Pair<>());
    }

    public Page() {
        resetMemory();
    }

    public List<Pair<KeyType, ValueType>> getData() {
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

    public void setPageID(int pageID) { this.pageID = pageID; }

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

//    public int getLSN() {
//        return (int) getData()[4];
//    }
//    public void SetLSN(Integer lsn) {
//        lsn = (int)getData()[4];
//    }
}