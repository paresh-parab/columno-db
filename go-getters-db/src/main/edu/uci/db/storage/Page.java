package main.edu.uci.db.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Page {

    public class Entry{
        private String name;
        private int rootID;

        public Entry(){
            rootID = -1;
            name = null;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getRootID() {
            return rootID;
        }

        public void setRootID(int rootID) {
            this.rootID = rootID;
        }
    }

    private int count;
    private List<Entry> data;
    private int pageID;
    int pinCount;
    boolean isDirty;

    private final ReadWriteLock rwlatch
            = new ReentrantReadWriteLock();
    private final Lock writeLock
            = rwlatch.writeLock();
    private final Lock readLock = rwlatch.readLock();

    private void resetMemory(){
        data = new ArrayList<>();
        for(int i=0; i<14; i++){
            data.add( new Entry()) ;
        }
    }

    public void addEntry(String name, int rootID, int index){
        data.get(index).setName(name);
        data.get(index).setRootID(rootID);
    }

    public void removeEntry(int index){
        data.remove(index);
    }

    public Page() {

        resetMemory();
    }

    public List<Entry> getData() {
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

    public int getPinCount() {
        return pinCount;
    }

    public void WUnlatch() {
        writeLock.unlock();
    }

    public void WLatch() {
        writeLock.lock();
    }

    public void RUnlatch() {
        readLock.unlock();
    }

    public void RLatch() {
        readLock.lock();
    }

//    public int getLSN() {
//        return (int) getData()[4];
//    }
//    public void SetLSN(Integer lsn) {
//        lsn = (int)getData()[4];
//    }
}