package main.storage.index;

import main.buffer.BufferPoolManager;
import main.common.OpType;
import main.common.Pair;
import main.storage.page.*;
import main.type.PageType;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static main.common.Constants.HEADER_PAGE_ID;
import static main.common.Constants.INVALID_PAGE_ID;

public class BPlusTree <KeyType, ValueType, KeyComparator extends Comparator>{


    //Mutex mutex ;
    private int rootLockedCount = 0;

    private String index_name;
    private KeyComparator comparator;
    private BufferPoolManager buffer_pool_manager;
    private int rootPageID;

    public BPlusTree(String name, BufferPoolManager buffer_pool_manager, KeyComparator comparator, int rootPageID) {
        this.index_name = name;
        this.buffer_pool_manager = buffer_pool_manager;
        this.comparator = comparator;
        this.rootPageID = rootPageID;

    }

    public BPlusTree(String name, BufferPoolManager buffer_pool_manager, KeyComparator comparator) {
        this(name, buffer_pool_manager, comparator, INVALID_PAGE_ID);
    }

    // Returns true if this B+ tree has no keys and values.
    public boolean isEmpty() {
        return rootPageID == INVALID_PAGE_ID;
    };

    // return the value associated with a given key
    public boolean getValue(KeyType key, List<ValueType> result) {
        //step 1. find page
        BPlusTreeLeafPage tar = findLeafPage(key,false, OpType.READ);
        if (tar == null)
            return false;
        //step 2. find value
        while(result.size()>1)
            result.remove(result.size()-1);
        boolean ret = tar.lookup(key, result.get(0), comparator );
        //step 3. unPin buffer pool
        buffer_pool_manager.unpinPage(tar.getPageID(), false);
        return ret;
    }

    // Insert a key-value pair into this B+ tree.
    public boolean insert(KeyType key, ValueType value ) {
        lockRootPageID(true);
        if (isEmpty()) {
            startNewTree(key,value);
            tryUnlockRootPageID(true);
            return true;
        }
        tryUnlockRootPageID(true);
        boolean res = insertIntoLeaf(key,value);
        return res;
    }

    private void startNewTree(KeyType key, ValueType value) {
        //step 1. ask for new page from buffer pool manager
        //buffer pool dependency
        try{
            int newPageID = 0;
            IndexPage<KeyType, ValueType> rootPage = (IndexPage<KeyType, ValueType>) buffer_pool_manager.newPage(PageType.INDEX);
            if(rootPage == null)
                throw new Exception("Unable to create fresh root page");

            BPlusTreeLeafPage root = new BPlusTreeLeafPage(rootPage);

            //step 2. update b+ tree's root page id
            root.init(newPageID, INVALID_PAGE_ID);
            rootPageID = newPageID;
            updateRootPageID(true);

            //step 3. insert entry directly into leaf page.
            root.insert(key,value,comparator);

            buffer_pool_manager.unpinPage(newPageID, true);
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }

    }

    /*
     * Insert constant key & value pair into leaf page
     * User needs to first find the right leaf page as insertion target, then look
     * through leaf page to see whether insert key exist or not. If exist, return
     * immdiately, otherwise insert entry. Remember to deal with split if necessary.
     * @return: since we only support unique key, if user try to insert duplicate
     * keys return false, otherwise return true.
     */

    private boolean insertIntoLeaf(KeyType key, ValueType value ) {
        BPlusTreeLeafPage leafPage = findLeafPage(key,false, OpType.INSERT);
        boolean exist = leafPage.lookup(key,value,comparator);
        if (exist) {
            return false;
        }
        leafPage.insert(key,value,comparator);
        if (leafPage.getSize() > leafPage.getMaxSize()) {//insert then split
            BPlusTreeLeafPage newLeafPage = split(leafPage);//unpin it in below func
            insertIntoParent(leafPage, (KeyType) newLeafPage.keyAt(0),newLeafPage);
        }
        return true;
    }

    /*
     * Split input page and return newly created page.
     * Using template N to represent either internal page or leaf page.
     * User needs to first ask for new page from buffer pool manager(NOTICE: throw
     * an "out of memory" exception if returned value is nullptr), then move half
     * of key & value pairs from input page to newly created page
     */

    private BPlusTreeLeafPage  split(BPlusTreeLeafPage node) {
        //step 1 ask for new page from buffer pool manager
        int newPageID = 0;
        IndexPage<KeyType, ValueType> newPage = (IndexPage<KeyType, ValueType>) buffer_pool_manager.newPage(PageType.INDEX);
        newPage.wLatch();
        //step 2 move half of key & value pairs from input page to newly created page
        BPlusTreeLeafPage newNode = new BPlusTreeLeafPage(newPage);
        newNode.init(newPageID, node.getParentPageID());
        node.moveHalfTo(newNode, buffer_pool_manager);
        return newNode;
    }

    private BPlusTreeInternalPage split(BPlusTreeInternalPage node) {
        //step 1 ask for new page from buffer pool manager
        int newPageID = 0;
        IndexPage<KeyType, ValueType> newPage = (IndexPage<KeyType, ValueType>) buffer_pool_manager.newPage(PageType.INDEX);
        newPage.wLatch();
        //step 2 move half of key & value pairs from input page to newly created page
        BPlusTreeInternalPage newNode = new BPlusTreeInternalPage(newPage);
        newNode.init(newPageID, node.getParentPageID());
        node.moveHalfTo(newNode, buffer_pool_manager);
        return newNode;
    }

    /*
     * Insert key & value pair into internal page after split
     * @param   old_node      input page from split() method
     * @param   key
     * @param   new_node      returned page from split() method
     * User needs to first find the parent page of old_node, parent node must be
     * adjusted to take info of new_node into account. Remember to deal with split
     * recursively if necessary.
     */

    private void insertIntoParent(BPlusTreePage oldNode, KeyType key, BPlusTreePage newNode) {
        try{
            if (oldNode.isRootPage()) {
                IndexPage<KeyType, ValueType> newPage = (IndexPage<KeyType, ValueType>) buffer_pool_manager.newPage(PageType.INDEX);
                if(newPage == null || newPage.getPinCount() == 1){
                    throw new Exception("Unable to create fresh page");
                }
                BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(newPage);
                newRoot.init(rootPageID);
                newRoot.populateNewRoot(oldNode.getPageID(), key, newNode.getPageID());
                oldNode.setParentPageID(rootPageID);
                newNode.setParentPageID(rootPageID);
                updateRootPageID();
                buffer_pool_manager.unpinPage(newRoot.getPageID(),true);
                return;
            }
            int parentID = oldNode.getParentPageID();
            IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) fetchPage(parentID);
            if(page == null)
                throw new Exception("Unable to create new Page");
            BPlusTreeInternalPage parent = new BPlusTreeInternalPage(page);
            newNode.setParentPageID(parentID);
            parent.insertNodeAfter(oldNode.getPageID(), key, newNode.getPageID());
            if (parent.getSize() > parent.getMaxSize()) {
                BPlusTreeInternalPage newLeafPage = split(parent);//new page need unpin
                insertIntoParent(parent, (KeyType) newLeafPage.keyAt(0),newLeafPage);
            }
            buffer_pool_manager.unpinPage(parentID, true);
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }


    }


    /*****************************************************************************
     * REMOVE
     *****************************************************************************/
    /*
     * Delete key & value pair associated with input key
     * If current tree is empty, return immdiately.
     * If not, User needs to first find the right leaf page as deletion target, then
     * delete entry from leaf page. Remember to deal with redistribute or merge if
     * necessary.
     */

    public  void remove(KeyType key) {
        if (isEmpty()) return;
        BPlusTreeLeafPage delTar = findLeafPage(key,false, OpType.DELETE);
        int curSize = delTar.removeAndDeleteRecord(key,comparator);
        if (curSize < delTar.getMinSize()) {
            coalesceOrRedistribute(delTar);
        }
    }

    /*
     * User needs to first find the sibling of input page. If sibling's size + input
     * page's size > page's max size, then redistribute. Otherwise, merge.
     * Using template N to represent either internal page or leaf page.
     * @return: true means target leaf page should be deleted, false means no
     * deletion happens
     */

    private <N extends BPlusTreePage> boolean coalesceOrRedistribute(N node) {
        if (node.isRootPage()) {
            boolean delOldRoot = adjustRoot(node);//make the child of N the new root of the tree and delete N
            return delOldRoot;
        }
        //Let N2 be the previous or next child of parent(N)
        N node2 = null;
        boolean isRightSib = findLeftSibling(node,node2);
        IndexPage<KeyType, ValueType> parent = (IndexPage<KeyType, ValueType>) fetchPage(node.getParentPageID());
        BPlusTreeInternalPage parentPage = new BPlusTreeInternalPage(parent);
        //if (entries in N and N2 can fit in a single node)
        if (node.getSize() + node2.getSize() <= node.getMaxSize()) {
            if (isRightSib) {
                //swap(
                N temp = node;
                node = node2;
                node2= temp;
                //assumption node is after node2
            }

            int removeIndex = parentPage.valueIndex(node.getPageID());
            coalesce(node2,node,parentPage,removeIndex);//unpin node,node2
            buffer_pool_manager.unpinPage(parentPage.getPageID(), true);
            return true;
        }
        /* Redistribution: borrow an entry from N2 */
        int nodeInParentIndex = parentPage.valueIndex(node.getPageID());
        redistribute(node2,node,nodeInParentIndex);//unpin node,node2
        buffer_pool_manager.unpinPage(parentPage.getPageID(), false);
        return false;
    }


    private <N extends BPlusTreePage> boolean findLeftSibling(N node, N sibling) {
        IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) fetchPage(node.getParentPageID());
        BPlusTreeInternalPage parent = new BPlusTreeInternalPage(page);
        int index = parent.valueIndex(node.getPageID());
        int siblingIndex = index - 1;
        if (index == 0) { //no left sibling
            siblingIndex = index + 1;
        }
        sibling = (N) crabingProtocalFetchPage((Integer) parent.valueAt(siblingIndex), OpType.DELETE,-1);
        buffer_pool_manager.unpinPage(parent.getPageID(), false);
        return index == 0;//index == 0 means sibling is right
    }



    private  <N extends BPlusTreePage> boolean coalesce( N neighborNode, N node, BPlusTreeInternalPage<KeyType,Integer, KeyComparator> parent,
                                               int index) {


        try{
            if(node.getSize() + neighborNode.getSize() > node.getMaxSize())
                throw new Exception("Unable to coalesce");
            //move later one to previous one
            node.moveAllTo(neighborNode,index,buffer_pool_manager);
            parent.remove(index);
            if (parent.getSize() <= parent.getMinSize()) {
                return coalesceOrRedistribute(parent);
            }
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return false;

    }




    private <N extends BPlusTreePage> void redistribute(N neighborNode, N node, int index) {
        if (index == 0) {
            neighborNode.moveFirstToEndOf(node,buffer_pool_manager);
        } else {
            neighborNode.moveLastToFrontOf(node, index, buffer_pool_manager);
        }
    }



    private boolean adjustRoot(BPlusTreePage oldRootNode) {
        try{
            if (oldRootNode.isLeafPage()) {// case 2
                if(oldRootNode.getSize() != 0 || oldRootNode.getParentPageID() != INVALID_PAGE_ID)
                    throw new Exception("Unable to adjust the root");
                rootPageID = INVALID_PAGE_ID;
                updateRootPageID();
                return true;
            }
            if (oldRootNode.getSize() == 1) {// case 1
                BPlusTreeInternalPage root = (BPlusTreeInternalPage) oldRootNode;
                int newRootId = (int) root.removeAndReturnOnlyChild();
                rootPageID = newRootId;
                updateRootPageID();
                // set the new root's parent id "INVALID_PAGE_ID"
                IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(newRootId);
                BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(page);
                newRoot.setParentPageID(INVALID_PAGE_ID);
                buffer_pool_manager.unpinPage(newRootId, true);
                return true;
            }
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return false;


    }




    public IndexIterator begin() {
        KeyType useless = null;
        BPlusTreeLeafPage start_leaf = findLeafPage(useless, true);
        tryUnlockRootPageID(false);
        return new IndexIterator(start_leaf, 0, buffer_pool_manager);
    }

    public IndexIterator begin(KeyType key) {
        BPlusTreeLeafPage startLeaf = findLeafPage(key);
        tryUnlockRootPageID(false);
        if (startLeaf == null) {
            return new IndexIterator(startLeaf, 0, buffer_pool_manager);
        }
        int idx = startLeaf.keyIndex(key,comparator);
        return new IndexIterator(startLeaf, idx, buffer_pool_manager);
    }




    public BPlusTreeLeafPage findLeafPage(KeyType key){
        return this.findLeafPage(key, false, OpType.READ);
    }

    public BPlusTreeLeafPage findLeafPage(KeyType key, boolean leftMost) {
        return this.findLeafPage(key, leftMost, OpType.READ);
    }

    public BPlusTreeLeafPage findLeafPage(KeyType key, boolean leftMost, OpType op ) {
        boolean exclusive = (op != OpType.READ);
        lockRootPageID(exclusive);
        if (isEmpty()) {
            tryUnlockRootPageID(exclusive);
            return null;
        }

        BPlusTreePage pointer =  crabingProtocalFetchPage(rootPageID,op,-1);
        int next;
        for (int cur = rootPageID;
             !pointer.isLeafPage();
             pointer = (BPlusTreeLeafPage) crabingProtocalFetchPage(next,op,cur),cur = next) {
            BPlusTreeInternalPage internalPage = (BPlusTreeInternalPage) pointer;
            if (leftMost) {
                next = (int) internalPage.valueAt(0);
            }else {
                next = (int) internalPage.lookup(key,comparator);
            }
        }
        return (BPlusTreeLeafPage) pointer;
    }

    private IndexPage<KeyType, ValueType> fetchPage(int page_id) {
        IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(page_id);
        return page;
    }


    private BPlusTreePage crabingProtocalFetchPage(int page_id, OpType op, int previous) {
        boolean exclusive = op != OpType.READ;
        IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(page_id);
        lock(exclusive,page);
        BPlusTreePage treePage = new BPlusTreePage(page);

        return treePage;
    }


    private void updateRootPageID(){
        updateRootPageID(false);
    }

    private void updateRootPageID(boolean insertRecord) {
        HeaderPage header_page = (HeaderPage) buffer_pool_manager.fetchPage(HEADER_PAGE_ID);
        if (insertRecord)
            header_page.insertRecord(index_name, rootPageID);
        else
            header_page.updateRecord(index_name, rootPageID);
        buffer_pool_manager.unpinPage(HEADER_PAGE_ID, true);
    }



    public int isBalanced(int pid) {

        try{
            if (isEmpty()) return 1;
            IndexPage<KeyType, ValueType> node = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(pid);
            BPlusTreePage pageNode = new BPlusTreePage(node);

            if (node == null) {
                throw new Exception("all page are pinned while isBalanced");
            }
            int ret = 0;
            if (!pageNode.isLeafPage())  {
                BPlusTreeInternalPage page = (BPlusTreeInternalPage) pageNode;
                int last = -2;
                for (int i = 0; i < page.getSize(); i++) {
                    int cur = isBalanced((Integer) page.valueAt(i));
                    if (cur>=0 && last == -2) {
                        last = cur;
                        ret = last + 1;
                    }else if (last != cur) {
                        ret = -1;
                        break;
                    }
                }
            }
            buffer_pool_manager.unpinPage(pid,false);
            return ret;
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return -1;

    }


    public boolean isPageCorr(int pid, Pair<KeyType,KeyType> out) {
        try{
            if (isEmpty()) return true;
            IndexPage<KeyType, ValueType> node = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(pid);
            BPlusTreePage pageNode = new BPlusTreePage(node);

            if (node == null) {
                throw new Exception("all page are pinned while isPageCorr");
            }
            boolean ret = true;
            if (pageNode.isLeafPage())  {
                BPlusTreeLeafPage page = (BPlusTreeLeafPage) pageNode;
                int size = page.getSize();
                ret = ret && (size >= pageNode.getMinSize() && size <= pageNode.getMaxSize());
                for (int i = 1; i < size; i++) {
                    if (comparator.compare(page.keyAt(i-1), page.keyAt(i)) > 0) {
                        ret = false;
                        break;
                    }
                }
                out = new Pair<KeyType,KeyType>((KeyType)page.keyAt(0),(KeyType)page.keyAt(size-1));
            } else {
                BPlusTreeInternalPage page = (BPlusTreeInternalPage) pageNode;
                int size = page.getSize();
                ret = ret && (size >= pageNode.getMinSize() && size <= pageNode.getMaxSize());
                Pair<KeyType,KeyType> left =  new Pair<>(),right = new Pair<>();
                for (int i = 1; i < size; i++) {
                    if (i == 1) {
                        ret = ret && isPageCorr((Integer) page.valueAt(0),left);
                    }
                    ret = ret && isPageCorr((Integer) page.valueAt(i),right);
                    ret = ret && (comparator.compare(page.keyAt(i) ,left.getValue())>0 && comparator.compare(page.keyAt(i), right.getKey())<=0);
                    ret = ret && (i == 1 || comparator.compare(page.keyAt(i-1) , page.keyAt(i)) < 0);
                    if (!ret) break;
                    left = right;
                }
                out = new Pair<KeyType,KeyType>((KeyType) page.keyAt(0), (KeyType) page.keyAt(size-1));
            }
            buffer_pool_manager.unpinPage(pid,false);
            return ret;
        }catch( Exception e){
            System.out.println("Program terminated due to exception: "+ e.getMessage());
            System.out.println(e.getStackTrace());
            System.exit(0);
        }
        return false;


    }


//    public boolean check(boolean forceCheck) {
//        if (!forceCheck && !openCheck) {
//            return true;
//        }
//        Pair<KeyType,KeyType> in = new Pair<>();
//        boolean isPageInOrderAndSizeCorr = isPageCorr(rootPageID, in);
//        boolean isBal = isBalanced(rootPageID) != 0;
//        boolean isAllUnpin = buffer_pool_manager.checkAllUnpined();
//        if (!isPageInOrderAndSizeCorr) System.out.println("problem in page order or page size");
//        if (!isBal) System.out.println("problem in balance");
//        if (!isAllUnpin) System.out.println("problem in page unpin");
//        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
//    }


    private void lock(boolean exclusive, Page page) {
        if (exclusive)
            page.wLatch();
        else
            page.rLatch();
    }

    private void unlock(boolean exclusive,Page page) {
        if (exclusive)
            page.wUnlatch();
        else
            page.rUnlatch();
    }

    private void unlock(boolean exclusive, int pageID) {
        IndexPage<KeyType, ValueType> page = (IndexPage<KeyType, ValueType>) buffer_pool_manager.fetchPage(pageID);
        unlock(exclusive, page);
        buffer_pool_manager.unpinPage(pageID, exclusive);
    }

    private final ReadWriteLock mutex
            = new ReentrantReadWriteLock();
    private final Lock writeLock
            = mutex.writeLock();
    private final Lock readLock = mutex.readLock();

    private void lockRootPageID(boolean exclusive) {
        if (exclusive) {
            writeLock.lock();
        } else {
            readLock.lock();
        }
        rootLockedCount++;
    }

    private void tryUnlockRootPageID(boolean exclusive) {
        if (rootLockedCount > 0) {
            if (exclusive) {
                writeLock.unlock();
            } else {
                readLock.unlock();
            }
            rootLockedCount--;
        }
    }

}

