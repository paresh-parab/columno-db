package main.storage.page;

import main.buffer.BufferPoolManager;
import main.common.Pair;

import java.lang.instrument.Instrumentation;
import java.util.Comparator;

import static main.common.Constants.INVALID_PAGE_ID;
import static main.common.Constants.PAGE_SIZE;
import static main.storage.index.IndexPageType.LEAF_PAGE;

public class BPlusTreeLeafPage <KeyType, ValueType, KeyComparator extends Comparator> extends BPlusTreePage {

    private static Instrumentation instrumentation;
    private int nextPageID;

    public BPlusTreeLeafPage(IndexPage<KeyType, ValueType> page) {
        super(page);
    }

    public void init(int pageID, int parentID){
        setPageType(LEAF_PAGE);
        setPageID(pageID);
        setParentPageID(parentID);
        setSize(0);
        setNextPageID(INVALID_PAGE_ID);
        //potential trouble
        //setMaxSize((int)((PAGE_SIZE - instrumentation.getObjectSize(this)) / instrumentation.getObjectSize(new Pair<KeyType, ValueType>( )) - 1));
        setMaxSize(PAGE_SIZE);
    }

    public void init(int pageID){
        this.init(pageID, INVALID_PAGE_ID);
    }

    // helper methods
    public int getNextPageID() {
        return nextPageID;
    }

    public void setNextPageID(int nextPageID){
        this.nextPageID = nextPageID;
    }

    public int keyIndex(KeyType key, KeyComparator comparator) {
        int start = 0, end = getSize()-1;

        // if not exists, return the index after the last one
        while (start < end) {
            int mid = start + (end - start) / 2;
            if (comparator.compare(key, ((Pair<KeyType, ValueType>)array.get(mid)).getKey()) > 0) {
                start = mid + 1;
            } else {
                end = mid;
            }
        }

        return start;
    }


    public KeyType keyAt(int index) {

        try{
            if(index >= 0 && index < getSize())
                return ((Pair<KeyType, ValueType>)array.get(index)).getKey();
            else
                throw new Exception("Invalid index");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }

    public ValueType valueAt(int index) {

        try{
            if(index >= 0 && index < getSize())
                return ((Pair<KeyType, ValueType>)array.get(index)).getValue();
            else
                throw new Exception("Invalid index");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }



    public Pair<KeyType, ValueType> getItem(int index) {
        try{
            if(index >= 0 && index < getSize())
                return (Pair<KeyType, ValueType>) array.get(index);
            else
                throw new Exception("Invalid index");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;
    }


    public int insert(KeyType key, ValueType value, KeyComparator comparator) {
        try{
            int idx = keyIndex(key,comparator); //first larger than key
            if(idx >= 0){
                increaseSize(1);
                array.add(idx, new Pair<>(key, value));
                array.remove(array.size()-1);
                return getSize();
            }
            else
                throw new Exception("Invalid index for insertion");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return -1;

    }

    public void moveHalfTo(BPlusTreeLeafPage recipient, BufferPoolManager buffer_pool_manager) {
        if(recipient != null){
            int total = getMaxSize() + 1;
            if(getSize() == total){
                int copyIdx = total/2;//7 is 4,5,6,7; 8 is 4,5,6,7,8

                for (int i = copyIdx; i < total; i++) {
                    recipient.array.set(i - copyIdx, new Pair<>((Pair<KeyType, ValueType>) array.get(i)));
                }
                //set pointer
                recipient.setNextPageID(getNextPageID());
                setNextPageID(recipient.getPageID());
                //set size, is odd, bigger is last part
                setSize(copyIdx);
                recipient.setSize(total - copyIdx);
            }
        }
    }

    public void copyHalfFrom(Pair<KeyType, ValueType> items, int size) {}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
    /*
     * For the given key, check to see whether it exists in the leaf page. If it
     * does, then store its corresponding value in input "value" and return true.
     * If the key does not exist, then return false
     */
    public boolean lookup(KeyType key, ValueType value, KeyComparator comparator) {
        int idx = keyIndex(key,comparator);
        if (idx < getSize() && comparator.compare(((Pair<KeyType, ValueType>)array.get(idx)).getKey(), key) == 0) {
            value = ((Pair<KeyType, ValueType>)array.get(idx)).getValue();
            return true;
        }
        return false;
    }

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /*
     * First look through leaf page to see whether delete key exist or not. If
     * exist, perform deletion, otherwise return immdiately.
     * NOTE: store key&value pair continuously after deletion
     * @return   page size after deletion
     */

    public int removeAndDeleteRecord( KeyType key,  KeyComparator comparator) {
        int firIdxLargerEqualThanKey = keyIndex(key,comparator);
        if (firIdxLargerEqualThanKey >= getSize() || comparator.compare(key, keyAt(firIdxLargerEqualThanKey)) != 0) {
            return getSize();
        }
        //quick deletion
        int tarIdx = firIdxLargerEqualThanKey;
        array.remove(tarIdx);
        array.add(new Pair<>());
        increaseSize(-1);
        return getSize();
    }

/*****************************************************************************
 * MERGE
 *****************************************************************************/
    /*
     * Remove all of key & value pairs from this page to "recipient" page, then
     * update next page id
     */

    public void moveAllTo(BPlusTreeLeafPage recipient, int rootID, BufferPoolManager buffer_pool_manager) {
        if(recipient != null){
            int startIdx = recipient.getSize();//7 is 4,5,6,7; 8 is 4,5,6,7,8
            for (int i = 0; i < getSize(); i++) {
                recipient.array.set(startIdx+i, new Pair<>((Pair<KeyType, ValueType>)array.get(i)));
            }
            //set pointer
            recipient.setNextPageID(getNextPageID());
            //set size, is odd, bigger is last part
            recipient.increaseSize(getSize());
            setSize(0);
        }
    }

//    void copyAllFrom(MappingType items, int size) {
//
//    }

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
    /*
     * Remove the first key & value pair from this page to "recipient" page, then
     * update relavent key & value pair in its parent page.
     */

    public void moveFirstToEndOf(BPlusTreeLeafPage recipient, BufferPoolManager buffer_pool_manager) {
        Pair<KeyType, ValueType> pair = getItem(0);
        increaseSize(-1);
        array.remove(0);
        array.add(new Pair<>());
        recipient.copyLastFrom(pair);
        //update relavent key & value pair in its parent page.
        IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(getParentPageID());
        BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page.getData();
        //it should be array.get(1) as parent is internal page and keys start from 1, values start from 0
        parent.setKeyAt(parent.valueIndex(getPageID()), ((Pair<KeyType, ValueType>)array.get(0)).getKey());
        buffer_pool_manager.unpinPage(getParentPageID(), true);
    }

    public void copyLastFrom(Pair<KeyType, ValueType> item) {
        if(getSize() + 1 <= getMaxSize()){
            array.set(getSize(), item);
            increaseSize(1);
        }

    }
    /*
     * Remove the last key & value pair from this page to "recipient" page, then
     * update relavent key & value pair in its parent page.
     */

    public  void moveLastToFrontOf(BPlusTreeLeafPage recipient, int parentIndex, BufferPoolManager buffer_pool_manager) {
        Pair<KeyType, ValueType> pair = getItem(getSize() - 1);
        increaseSize(-1);
        recipient.copyFirstFrom(pair, parentIndex, buffer_pool_manager);
    }

    public void copyFirstFrom(Pair<KeyType, ValueType> item, int parentIndex, BufferPoolManager buffer_pool_manager) {
        if(getSize() + 1 < getMaxSize()){
            increaseSize(1);
            array.add(0, item);
            array.remove(array.size()-1);
            IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(getParentPageID());
            BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page.getData();
            parent.setKeyAt(parentIndex, ((Pair<KeyType, ValueType>)array.get(0)).getKey());
            buffer_pool_manager.unpinPage(getParentPageID(), true);
        }
    }

    public String toString(boolean verbose) {
        if (getSize() == 0) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        if (verbose) {
            builder.append("[pageId: " + getPageID() + " parentId: " + getParentPageID()
                    + "]<" + getSize() + "> ");
        }

        int entry = verbose ? 0 : 1;
        int end = getSize();
        boolean first = true;
        while (entry < end) {
            if (first) {
                first = false;
            } else {
                builder.append(" ");
            }
            builder.append(((Pair<KeyType, ValueType>)array.get(entry)).getKey().toString());
            if (verbose) {
                builder.append(((Pair<KeyType, ValueType>)array.get(entry)).getValue().toString());
            }
            ++entry;
        }
        return builder.toString();
    }

}

