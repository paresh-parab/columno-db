package main.storage;

import java.lang.instrument.Instrumentation;
import java.util.Comparator;

import static main.storage.Constants.INVALID_PAGE_ID;
import static main.storage.Constants.PAGE_SIZE;
import static main.storage.IndexPageType.INTERNAL_PAGE;

public class BPlusTreeInternalPage<KeyType, ValueType, KeyComparator extends Comparator> extends BPlusTreePage{

    private static Instrumentation instrumentation;

    public void init(int pageID, int parentID){
        setPageType(INTERNAL_PAGE);
        setPageID(pageID);
        setParentPageID(parentID);
        setSize(0);
        //potential trouble
        //setMaxSize((int)((PAGE_SIZE - instrumentation.getObjectSize(this)) / instrumentation.getObjectSize(new Pair<KeyType, ValueType>( )) - 1));
        setMaxSize(PAGE_SIZE-1);
    }

    public void init(int page_id){
        this.init(page_id, INVALID_PAGE_ID);
    }

    public BPlusTreeInternalPage(Page<KeyType, ValueType> page){
        super(page);
    }

    KeyType keyAt(int index) {
        if(index >= 0 && index < getSize())
            return ((Pair<KeyType, ValueType>)array.get(index)).getKey();
        else
            throw new Exception("Invalid index");
    }

    void setKeyAt(int index, KeyType key) {
        // first key is invalid
        if(index > 0 && index < getSize())
            ((Pair<KeyType, ValueType>)array.get(index)).setKey(key);
        else
            throw new Exception("Invalid index");
    }

    int valueIndex(ValueType value) {
        for (int i = 0; i < getSize(); ++i) {
            if (((Pair<KeyType, ValueType>)array.get(i)).getValue() == value) {
                return i;
            }
        }
        return -1;
    }

    ValueType valueAt(int index) {
        if(index >= 0 && index < getSize())
            return ((Pair<KeyType, ValueType>)array.get(index)).getValue();
        else
            throw new Exception("Invalid index");
    }

    ValueType lookup(KeyType key, KeyComparator comparator) {
        if(getSize() > 1){
            int start = 1, end = getSize();

            while (start < end) {
                int mid = start + (end - start) / 2;
                if (comparator.compare(key, keyAt(mid)) > 0) {
                    start = mid + 1;

                } else {
                    end = mid;
                }
            }
            return valueAt(start - 1);
        }
        else {
            throw new Exception("Invalid index");
        }
    }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
    /*
     * Populate new root page with old_value + new_key & new_value
     * When the insertion cause overflow from leaf page all the way upto the root
     * page, you should create a new root page and populate its elements.
     * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
     */

    void populateNewRoot(ValueType old_value,KeyType new_key,ValueType new_value) {
        ((Pair<KeyType, ValueType>)array.get(0)).setValue(old_value);
        ((Pair<KeyType, ValueType>)array.get(1)).setKey(new_key);
        ((Pair<KeyType, ValueType>)array.get(1)).setValue(new_value);
        setSize(2);

    }

    /*
     * Insert new_key & new_value pair right after the pair with its value ==
     * old_value
     * @return:  new size after insertion
     */

    int insertNodeAfter( ValueType old_value, KeyType new_key, ValueType new_value) {
        int idx = valueIndex(old_value) + 1;
        if(idx > 0){
            increaseSize(1);
            int curSize = getSize();
            array.add(idx, new Pair<KeyType, ValueType>(new_key,  new_value));
            array.remove(array.size()-1);
            return curSize;
        }
        return getSize();
    }

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
    /*
     * Remove half of key & value pairs from this page to "recipient" page
     */

    void moveHalfTo( BPlusTreeInternalPage recipient, BufferPoolManager buffer_pool_manager) {
        if(recipient != null){
            int total = getMaxSize() + 1;
            if(getSize() == total) {
                //copy last half
                int copyIdx = (total) / 2;//max:4 x,1,2,3,4 -> 2,3,4
                int recipPageId = recipient.getPageID();
                for (int i = copyIdx; i < total; i++) {
                    recipient.array.set(i - copyIdx, new Pair<>((Pair<KeyType, ValueType>)array.get(i)));
                    //update children's parent page
                    Page childRawPage = buffer_pool_manager.fetchPage(((Pair<KeyType, ValueType>)array.get(i)).getValue());
                    BPlusTreePage childTreePage = new BPlusTreePage(childRawPage);
                    childTreePage.setParentPageID(recipPageId);
                    buffer_pool_manager.unpinPage(((Pair<KeyType, ValueType>)array.get(i)).getValue(), true);
                }
                //set size,is odd, bigger is last part
                setSize(copyIdx);
                recipient.setSize(total - copyIdx);
            }
        }
    }

    void copyHalfFrom(Pair<KeyType, ValueType> items, int size, BufferPoolManager buffer_pool_manager) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
    /*
     * Remove the key & value pair in internal page according to input index(a.k.a
     * array offset)
     * NOTE: store key&value pair continuously after deletion
     */

    void remove(int index) {
        if(index >= 0 && index < getSize()){
            array.remove(index);
            array.add(new Pair<>());
            increaseSize(-1);
        }

    }

    /*
     * Remove the only key & value pair in internal page and return the value
     * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
     */
    ValueType  removeAndReturnOnlyChild() {
        try{
            ValueType ret = valueAt(0);
            increaseSize(-1);
            if(getSize() == 0)
                return ret;
            else
                throw new Exception("There are more than one children");
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return null;

    }
/*****************************************************************************
 * MERGE
 *****************************************************************************/
    /*
     * Remove all of key & value pairs from this page to "recipient" page, then
     * update relavent key & value pair in its parent page.
     */

    void moveAllTo( BPlusTreeInternalPage recipient, int index_in_parent, BufferPoolManager buffer_pool_manager) {
        int start = recipient.getSize();
        int recipPageId = recipient.getPageID();
        // first find parent
        Page page = buffer_pool_manager.fetchPage(getParentPageID());
        if(page != null){
            BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page.getData();

            // the separation key from parent
            setKeyAt(0, (KeyType) parent.keyAt(index_in_parent));
            buffer_pool_manager.unpinPage(parent.getPageID(), false);
            for (int i = 0; i < getSize(); ++i) {
                recipient.array.set(start+i, new Pair<>((Pair<KeyType, ValueType>)array.get(i)));

                //update children's parent page
                Page childRawPage = buffer_pool_manager.fetchPage(((Pair<KeyType, ValueType>)array.get(i)).getValue());
                BPlusTreePage childTreePage = (BPlusTreePage) childRawPage.getData();
                childTreePage.setParentPageID(recipPageId);
                buffer_pool_manager.unpinPage(((Pair<KeyType, ValueType>)array.get(i)).getValue(), true);
            }
            //update relavent key & value pair in its parent page.
            recipient.setSize(start + getSize());
            if(recipient.getSize() <= getMaxSize())
                setSize(0);
        }

    }


    void copyAllFrom(Pair<KeyType, ValueType> items, int size, BufferPoolManager buffer_pool_manager) {

    }

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
    /*
     * Remove the first key & value pair from this page to tail of "recipient"
     * page, then update relavent key & value pair in its parent page.
     */

    void moveFirstToEndOf( BPlusTreeInternalPage recipient, BufferPoolManager buffer_pool_manager) {
        Pair<KeyType, ValueType> pair = new Pair<>(keyAt(0), valueAt(0));
        increaseSize(-1);
        array.remove(0);
        array.add(new Pair<>());
        recipient.copyLastFrom(pair, buffer_pool_manager);
        // update child parent page id
        int childPageId = (int) pair.getValue();
        Page page = buffer_pool_manager.fetchPage(childPageId);
        //assert (page != nullptr);
        BPlusTreePage child = new BPlusTreePage(page);
        child.setParentPageID(recipient.getPageID());
        //assert(child.getParentPageID() == recipient.GetPageID());
        buffer_pool_manager.unpinPage(child.getPageID(), true);
        //update relavent key & value pair in its parent page.
        page = buffer_pool_manager.fetchPage(getParentPageID());
        BPlusTreeInternalPage parent = new BPlusTreeInternalPage(page);
        parent.setKeyAt(parent.valueIndex(getPageID()), ((Pair<KeyType, ValueType>)array.get(0)).getKey());
        buffer_pool_manager.unpinPage(getParentPageID(), true);
    }


    void copyLastFrom(Pair<KeyType, ValueType> pair, BufferPoolManager buffer_pool_manager) {
        //assert(GetSize() + 1 <= GetMaxSize());
        array.set(getSize(), pair);
        increaseSize(1);
    }

    /*
     * Remove the last key & value pair from this page to head of "recipient"
     * page, then update relavent key & value pair in its parent page.
     */

    void moveLastToFrontOf(BPlusTreeInternalPage recipient, int parent_index, BufferPoolManager buffer_pool_manager) {
        Pair<KeyType, ValueType> pair = new Pair<>(keyAt(getSize() - 1),valueAt(getSize() - 1));
        increaseSize(-1);
        recipient.copyFirstFrom(pair, parent_index, buffer_pool_manager);
    }


    void copyFirstFrom(Pair<KeyType, ValueType> pair, int parent_index, BufferPoolManager buffer_pool_manager) {
        //assert(GetSize() + 1 < GetMaxSize());
        increaseSize(1);
        array.add(0, pair);
        array.remove(array.size()-1);
        // update child parent page id
        int childPageId = (int) pair.getValue();
        Page page = buffer_pool_manager.fetchPage(childPageId);
        //assert (page != nullptr);
        BPlusTreePage child = new BPlusTreePage(page);
        child.setParentPageID(getPageID());
        //assert(child->GetParentPageId() == GetPageId());
        buffer_pool_manager.unpinPage(child.getPageID(), true);
        //update relavent key & value pair in its parent page.
        page = buffer_pool_manager.fetchPage(getParentPageID());
        BPlusTreeInternalPage parent = new BPlusTreeInternalPage(page);
        parent.setKeyAt(parent_index, ((Pair<KeyType, ValueType>)array.get(0)).getKey());
        buffer_pool_manager.unpinPage(getParentPageID(), true);
    }



    String toString(boolean verbose) {
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
