package main.edu.uci.db.storage;

import java.util.Comparator;

public class BPlusTreeLeafPage <KeyType, ValueType, KeyComparator extends Comparator> extends BPlusTreePage {

//    private static Instrumentation instrumentation;
//    private int nextPageID;
//    private List<Pair<KeyType, ValueType>> array ; //[0];
//
//    public void init(int pageID, int parentID){
//        setPageType(LEAF_PAGE);
//        setPageID(pageID);
//        setParentPageID(parentID);
//        setSize(0);
//        setNextPageID(INVALID_PAGE_ID);
//        //potential trouble
//        setMaxSize((int)((PAGE_SIZE - instrumentation.getObjectSize(this)) / instrumentation.getObjectSize(new Pair<KeyType, ValueType>( )) - 1));
//
//    }
//
//    public void init(int pageID){
//        this.init(pageID, INVALID_PAGE_ID);
//    }
//
//    // helper methods
//    public int getNextPageID() {
//        return nextPageID;
//    }
//
//    public void setNextPageID(int nextPageID){
//        this.nextPageID = nextPageID;
//    }
//
//    public KeyType keyAt(int index) throws Exception {
//        if(index >= 0 && index < getSize())
//            return array.get(index).getKey();
//        else
//            throw new Exception("Invalid index");
//    }
//
//    public ValueType valueAt(int index) throws Exception {
//        if(index >= 0 && index < getSize())
//            return array.get(index).getValue();
//        else
//            throw new Exception("Invalid index");
//    }
//
//    public int keyIndex(KeyType key, KeyComparator comparator) {
//        int start = 0, end = getSize();
//
//        // if not exists, return the index after the last one
//        while (start < end) {
//            int mid = start + (end - start) / 2;
//            if (comparator.compare(key, array.get(mid).getKey()) > 0) {
//                start = mid + 1;
//            } else {
//                end = mid;
//            }
//        }
//
//        return start;
//    }
//
//    public Pair<KeyType, ValueType> getItem(int index) throws Exception {
//        if(index >= 0 && index < getSize())
//            return array.get(index);
//        else
//            throw new Exception("Invalid index");
//    }
//
//
//    int insert(KeyType key, ValueType value, KeyComparator comparator) {
//        int idx = keyIndex(key,comparator); //first larger than key
//        if(idx >= 0){
//            increaseSize(1);
//            int curSize = getSize();
//            for (int i = curSize - 1; i > idx; i--) {
//                array.get(i).setKey(array.get(i - 1).getKey());
//                array.get(i).setValue(array.get(i - 1).getValue());
//            }
//            array.get(idx).setKey(key);
//            array.get(idx).setValue(value);
//            return curSize;
//        }
//        //potential trouble
//        return getSize();
//
//    }
//
//    void moveHalfTo(BPlusTreeLeafPage recipient, BufferPoolManager buffer_pool_manager) {
//        if(recipient != null){
//            int total = getMaxSize() + 1;
//            if(getSize() == total){
//                int copyIdx = total/2;//7 is 4,5,6,7; 8 is 4,5,6,7,8
//                for (int i = copyIdx; i < total; i++) {
//                    ((Pair<KeyType, ValueType>)recipient.array.get(i - copyIdx)).setKey(array.get(i).getKey());
//                    ((Pair<KeyType, ValueType>)recipient.array.get(i - copyIdx)).setValue(array.get(i).getValue());
//                }
//                //set pointer
//                recipient.setNextPageID(getNextPageID());
//                setNextPageID(recipient.getPageID());
//                //set size, is odd, bigger is last part
//                setSize(copyIdx);
//                recipient.setSize(total - copyIdx);
//            }
//        }
//    }
//
//    void copyHalfFrom(Pair<KeyType, ValueType> items, int size) {}
//
///*****************************************************************************
// * LOOKUP
// *****************************************************************************/
//    /*
//     * For the given key, check to see whether it exists in the leaf page. If it
//     * does, then store its corresponding value in input "value" and return true.
//     * If the key does not exist, then return false
//     */
//    boolean lookup(KeyType key, ValueType value, KeyComparator comparator) {
//        int idx = keyIndex(key,comparator);
//        if (idx < getSize() && comparator.compare(array.get(idx).getKey(), key) == 0) {
//            value = array.get(idx).getValue();
//            return true;
//        }
//        return false;
//    }
//
///*****************************************************************************
// * REMOVE
// *****************************************************************************/
//    /*
//     * First look through leaf page to see whether delete key exist or not. If
//     * exist, perform deletion, otherwise return immdiately.
//     * NOTE: store key&value pair continuously after deletion
//     * @return   page size after deletion
//     */
//
//    int removeAndDeleteRecord( KeyType key,  KeyComparator comparator) throws Exception {
//        int firIdxLargerEqualThanKey = keyIndex(key,comparator);
//        if (firIdxLargerEqualThanKey >= getSize() || comparator.compare(key, keyAt(firIdxLargerEqualThanKey)) != 0) {
//            return getSize();
//        }
//        //quick deletion
//        int tarIdx = firIdxLargerEqualThanKey;
//        array.remove(tarIdx );
//        increaseSize(-1);
//        return getSize();
//    }
//
///*****************************************************************************
// * MERGE
// *****************************************************************************/
//    /*
//     * Remove all of key & value pairs from this page to "recipient" page, then
//     * update next page id
//     */
//
//    void moveAllTo(BPlusTreeLeafPage recipient, int rootID, BufferPoolManager buffer_pool_manager) {
//        if(recipient != null){
//            int startIdx = recipient.getSize();//7 is 4,5,6,7; 8 is 4,5,6,7,8
//            for (int i = 0; i < getSize(); i++) {
//                ((Pair<KeyType, ValueType>)recipient.array.get(startIdx + i)).setKey( array.get(i).getKey());
//                ((Pair<KeyType, ValueType>)recipient.array.get(startIdx + i)).setValue( array.get(i).getValue());
//            }
//            //set pointer
//            recipient.setNextPageID(getNextPageID());
//            //set size, is odd, bigger is last part
//            recipient.increaseSize(getSize());
//            setSize(0);
//        }
//    }
//
////    void copyAllFrom(MappingType items, int size) {
////
////    }
//
///*****************************************************************************
// * REDISTRIBUTE
// *****************************************************************************/
//    /*
//     * Remove the first key & value pair from this page to "recipient" page, then
//     * update relavent key & value pair in its parent page.
//     */
//
//    void moveFirstToEndOf(BPlusTreeLeafPage recipient, BufferPoolManager buffer_pool_manager) throws Exception {
//        Pair<KeyType, ValueType> pair = getItem(0);
//        increaseSize(-1);
//        array.remove(0);
//        recipient.copyLastFrom(pair);
//        //update relavent key & value pair in its parent page.
//        Page page = buffer_pool_manager.fetchPage(getParentPageID());
//        BPlusTreeInternalPage parent = page->getData();
//        parent.setKeyAt(parent.valueIndex(getPageID()), array.get(0).getKey());
//        buffer_pool_manager.unpinPage(getParentPageID(), true);
//    }
//
//    void copyLastFrom(Pair<KeyType, ValueType> item) {
//        if(getSize() + 1 <= getMaxSize()){
//            array.add(item);
//            increaseSize(1);
//        }
//
//    }
//    /*
//     * Remove the last key & value pair from this page to "recipient" page, then
//     * update relavent key & value pair in its parent page.
//     */
//
//    void moveLastToFrontOf(BPlusTreeLeafPage recipient, int parentIndex, BufferPoolManager buffer_pool_manager) throws Exception {
//        Pair<KeyType, ValueType> pair = getItem(getSize() - 1);
//        increaseSize(-1);
//        recipient.copyFirstFrom(pair, parentIndex, buffer_pool_manager);
//    }
//
//    void copyFirstFrom(Pair<KeyType, ValueType> item, int parentIndex, BufferPoolManager buffer_pool_manager) {
//        if(getSize() + 1 < getMaxSize()){
//            increaseSize(1);
//            array.add(0, item);
//            Page *page = buffer_pool_manager.fetchPage(getParentPageID());
//            B_PLUS_TREE_INTERNAL_PAGE parent = page.getData();
//            parent.setKeyAt(parentIndex, array.get(0).getKey());
//            buffer_pool_manager.unpinPage(getParentPageID(), true);
//        }
//    }
//
//    String toString(boolean verbose) {
//        if (getSize() == 0) {
//            return "";
//        }
//
//        StringBuilder builder = new StringBuilder();
//        if (verbose) {
//            builder.append("[pageId: " + getPageID() + " parentId: " + getParentPageID()
//                    + "]<" + getSize() + "> ");
//        }
//
//        int entry = verbose ? 0 : 1;
//        int end = getSize();
//        boolean first = true;
//        while (entry < end) {
//            if (first) {
//                first = false;
//            } else {
//                builder.append(" ");
//            }
//            builder.append(array.get(entry).getKey().toString());
//            if (verbose) {
//                builder.append(array.get(entry).getValue().toString());
//            }
//            ++entry;
//        }
//        return builder.toString();
//    }

}

