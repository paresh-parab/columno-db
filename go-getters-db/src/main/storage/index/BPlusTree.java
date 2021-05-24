package main.storage.index;

import main.buffer.BufferPoolManager;
import main.common.OpType;
import main.common.Pair;
import main.storage.page.*;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static main.common.Constants.INVALID_PAGE_ID;
import static main.common.OpType.*;

public class BPlusTree <KeyType, ValueType, KeyComparator extends Comparator>{


//    Mutex mutex ;
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
    boolean isEmpty() {
        return rootPageID == INVALID_PAGE_ID;
    };

    // return the value associated with a given key
    boolean getValue(KeyType key, List<ValueType> result) throws Exception {
        return this.getValue(key, result, null);
    }

    boolean getValue(KeyType key, List<ValueType> result, Transaction transaction) throws Exception {
        //step 1. find page
        BPlusTreeLeafPage tar = findLeafPage(key,false, READ,transaction);
        if (tar == null)
            return false;
        //step 2. find value
        while(result.size()>1)
            result.remove(result.size()-1);
        boolean ret = tar.lookup(key, result.get(0), comparator );
        //step 3. unPin buffer pool
        freePagesInTransaction(false,transaction,tar.getPageID());
        buffer_pool_manager.unpinPage(tar.getPageID(), false);
        //assert(buffer_pool_manager_->CheckAllUnpined());
        return ret;
    }

    // Insert a key-value pair into this B+ tree.
    boolean insert(KeyType key, ValueType value) throws Exception {
        return this.insert(key, value, null);
    }

    boolean insert(KeyType key, ValueType value, Transaction transaction) throws Exception {
        lockRootPageID(true);
        if (isEmpty()) {
            startNewTree(key,value);
            tryUnlockRootPageID(true);
            return true;
        }
        tryUnlockRootPageID(true);
        boolean res = insertIntoLeaf(key,value,transaction);
        //assert(Check());
        return res;
    }

    private void startNewTree(KeyType key, ValueType value) throws Exception {
        //step 1. ask for new page from buffer pool manager
        //buffer pool dependency
        int newPageID = 0;
        Page rootPage = buffer_pool_manager.newPage(newPageID);
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
    }

    /*
     * Insert constant key & value pair into leaf page
     * User needs to first find the right leaf page as insertion target, then look
     * through leaf page to see whether insert key exist or not. If exist, return
     * immdiately, otherwise insert entry. Remember to deal with split if necessary.
     * @return: since we only support unique key, if user try to insert duplicate
     * keys return false, otherwise return true.
     */
    private boolean insertIntoLeaf(KeyType key, ValueType value) throws Exception {
        return this.insertIntoLeaf(key, value, null);
    }

    private boolean insertIntoLeaf(KeyType key, ValueType value, Transaction transaction) throws Exception {
        BPlusTreeLeafPage leafPage = findLeafPage(key,false,INSERT,transaction);
        boolean exist = leafPage.lookup(key,value,comparator);
        if (exist) {
            //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
            freePagesInTransaction(true,transaction);
            return false;
        }
        leafPage.insert(key,value,comparator);
        if (leafPage.getSize() > leafPage.getMaxSize()) {//insert then split
            BPlusTreeLeafPage newLeafPage = split(leafPage,transaction);//unpin it in below func
            insertIntoParent(leafPage, (KeyType) newLeafPage.keyAt(0),newLeafPage,transaction);
        }
        //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
        freePagesInTransaction(true,transaction);
        return true;
    }

    /*
     * Split input page and return newly created page.
     * Using template N to represent either internal page or leaf page.
     * User needs to first ask for new page from buffer pool manager(NOTICE: throw
     * an "out of memory" exception if returned value is nullptr), then move half
     * of key & value pairs from input page to newly created page
     */

    private BPlusTreeLeafPage  split(BPlusTreeLeafPage node, Transaction transaction) {
        //step 1 ask for new page from buffer pool manager
        int newPageID = 0;
        Page newPage = buffer_pool_manager.newPage(newPageID);
        //assert(newPage != nullptr);
        newPage.wLatch();
        transaction.addIntoPageSet(newPage);
        //step 2 move half of key & value pairs from input page to newly created page
        BPlusTreeLeafPage newNode = new BPlusTreeLeafPage(newPage);
        newNode.init(newPageID, node.getParentPageID());
        node.moveHalfTo(newNode, buffer_pool_manager);
        //fetch page and new page need to unpin page(do it outside)
        return newNode;
    }

    private BPlusTreeInternalPage split(BPlusTreeInternalPage node, Transaction transaction) {
        //step 1 ask for new page from buffer pool manager
        int newPageID = 0;
        Page newPage = buffer_pool_manager.newPage(newPageID);
        //assert(newPage != nullptr);
        newPage.wLatch();
        transaction.addIntoPageSet(newPage);
        //step 2 move half of key & value pairs from input page to newly created page
        BPlusTreeInternalPage newNode = new BPlusTreeInternalPage(newPage);
        newNode.init(newPageID, node.getParentPageID());
        node.moveHalfTo(newNode, buffer_pool_manager);
        //fetch page and new page need to unpin page(do it outside)
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

    private void insertIntoParent(BPlusTreePage oldNode, KeyType key, BPlusTreePage newNode) throws Exception {
        insertIntoParent(oldNode, key, newNode, null);
    }

    private void insertIntoParent(BPlusTreePage oldNode, KeyType key, BPlusTreePage newNode, Transaction transaction) throws Exception {
        if (oldNode.isRootPage()) {
            Page newPage = buffer_pool_manager.newPage(rootPageID);
            if(newPage == null || newPage.getPinCount() == 1){
                throw new Exception("Unable to create fresh page");
            }
            BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(newPage);
            newRoot.init(rootPageID);
            newRoot.populateNewRoot(oldNode.getPageID(), key, newNode.getPageID());
            oldNode.setParentPageID(rootPageID);
            newNode.setParentPageID(rootPageID);
            updateRootPageID();
            //fetch page and new page need to unpin page
            //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
            buffer_pool_manager.unpinPage(newRoot.getPageID(),true);
            return;
        }
        int parentID = oldNode.getParentPageID();
        BPlusTreePage page = fetchPage(parentID);
        if(page == null)
            throw new Exception("Unable to create new Page");
        BPlusTreeInternalPage parent = new BPlusTreeInternalPage(page);
        newNode.setParentPageID(parentID);
        //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
        //insert new node after old node
        parent.insertNodeAfter(oldNode.getPageID(), key, newNode.getPageID());
        if (parent.getSize() > parent.getMaxSize()) {
            //begin /* Split Parent */
            BPlusTreeInternalPage newLeafPage = split(parent,transaction);//new page need unpin
            insertIntoParent(parent, (KeyType) newLeafPage.keyAt(0),newLeafPage,transaction);
        }
        buffer_pool_manager.unpinPage(parentID, true);
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

    public void remove(KeyType key) throws Exception {
        remove(key, null);
    }
    void remove(KeyType key, Transaction transaction) throws Exception {
        if (isEmpty()) return;
        BPlusTreeLeafPage delTar = findLeafPage(key,false, DELETE, transaction);
        int curSize = delTar.removeAndDeleteRecord(key,comparator);
        if (curSize < delTar.getMinSize()) {
            coalesceOrRedistribute(delTar,transaction);
        }
        freePagesInTransaction(true,transaction);
        //assert(Check());
    }

    /*
     * User needs to first find the sibling of input page. If sibling's size + input
     * page's size > page's max size, then redistribute. Otherwise, merge.
     * Using template N to represent either internal page or leaf page.
     * @return: true means target leaf page should be deleted, false means no
     * deletion happens
     */
    private <N extends BPlusTreePage> boolean coalesceOrRedistribute(N node) throws Exception {
        return coalesceOrRedistribute(node, null);
    }
    private <N extends BPlusTreePage> boolean coalesceOrRedistribute(N node, Transaction transaction) throws Exception {
        //if (N is the root and N has only one remaining child)
        if (node.isRootPage()) {
            boolean delOldRoot = adjustRoot(node);//make the child of N the new root of the tree and delete N
            if (delOldRoot) {transaction.addIntoDeletedPageSet(node.getPageID());}
            return delOldRoot;
        }
        //Let N2 be the previous or next child of parent(N)
        N node2 = null;
        boolean isRightSib = findLeftSibling(node,node2,transaction);
        BPlusTreePage parent = fetchPage(node.getParentPageID());
        BPlusTreeInternalPage parentPage = (BPlusTreeInternalPage)parent;
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
            coalesce(node2,node,parentPage,removeIndex,transaction);//unpin node,node2
            buffer_pool_manager.unpinPage(parentPage.getPageID(), true);
            return true;
        }
        /* Redistribution: borrow an entry from N2 */
        int nodeInParentIndex = parentPage.valueIndex(node.getPageID());
        redistribute(node2,node,nodeInParentIndex);//unpin node,node2
        buffer_pool_manager.unpinPage(parentPage.getPageID(), false);
        return false;
    }


    private <N extends BPlusTreePage> boolean findLeftSibling(N node, N sibling, Transaction transaction) throws Exception {
        BPlusTreePage page = fetchPage(node.getParentPageID());
        BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page;
        int index = parent.valueIndex(node.getPageID());
        int siblingIndex = index - 1;
        if (index == 0) { //no left sibling
            siblingIndex = index + 1;
        }
        sibling = (N) crabingProtocalFetchPage((Integer) parent.valueAt(siblingIndex), DELETE,-1,transaction);
        buffer_pool_manager.unpinPage(parent.getPageID(), false);
        return index == 0;//index == 0 means sibling is right
    }

    /*
     * Move all the key & value pairs from one page to its sibling page, and notify
     * buffer pool manager to delete this page. Parent page must be adjusted to
     * take info of deletion into account. Remember to deal with coalesce or
     * redistribute recursively if necessary.
     * Using template N to represent either internal page or leaf page.
     * @param   neighbor_node      sibling page of input "node"
     * @param   node               input from method coalesceOrRedistribute()
     * @param   parent             parent page of input "node"
     * @return  true means parent node should be deleted, false means no deletion
     * happend
     */
    private  <N extends BPlusTreePage> boolean coalesce( N neighbor_node, N node, BPlusTreeInternalPage<KeyType,Integer, KeyComparator> parent,
                                                        int index ) throws Exception{
        return coalesce(neighbor_node, node, parent, index, null);
    }

    private  <N extends BPlusTreePage> boolean coalesce( N neighbor_node, N node, BPlusTreeInternalPage<KeyType,Integer, KeyComparator> parent,
                                               int index, Transaction transaction) throws Exception {
        //assumption neighbor_node is before node
        if(node.getSize() + neighbor_node.getSize() > node.getMaxSize())
            throw new Exception("Unable to coalesce");
        //move later one to previous one
        node.moveAllTo(neighbor_node,index,buffer_pool_manager);
        transaction.addIntoDeletedPageSet(node.getPageID());
        parent.remove(index);
        if (parent.getSize() <= parent.getMinSize()) {
            return coalesceOrRedistribute(parent,transaction);
        }
        return false;
    }


    /*
     * Redistribute key & value pairs from one page to its sibling page. If index ==
     * 0, move sibling page's first key & value pair into end of input "node",
     * otherwise move sibling page's last key & value pair into head of input
     * "node".
     * Using template N to represent either internal page or leaf page.
     * @param   neighbor_node      sibling page of input "node"
     * @param   node               input from method coalesceOrRedistribute()
     */
    private <N extends BPlusTreePage> void redistribute(N neighborNode, N node, int index) {
        if (index == 0) {
            neighborNode.moveFirstToEndOf(node,buffer_pool_manager);
        } else {
            neighborNode.moveLastToFrontOf(node, index, buffer_pool_manager);
        }
    }

    /*
     * Update root page if necessary
     * NOTE: size of root page can be less than min size and this method is only
     * called within coalesceOrRedistribute() method
     * case 1: when you delete the last element in root page, but root page still
     * has one last child
     * case 2: when you delete the last element in whole b+ tree
     * @return : true means root page should be deleted, false means no deletion
     * happend
     */
    private boolean adjustRoot(BPlusTreePage oldRootNode) throws Exception {
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
            Page page = buffer_pool_manager.fetchPage(newRootId);
            //assert(page != nullptr);
            BPlusTreeInternalPage newRoot = new BPlusTreeInternalPage(page);
            newRoot.setParentPageID(INVALID_PAGE_ID);
            buffer_pool_manager.unpinPage(newRootId, true);
            return true;
        }
        return false;
    }

    /*****************************************************************************
     * INDEX ITERATOR
     *****************************************************************************/
    /*
     * Input parameter is void, find the leaftmost leaf page first, then construct
     * index iterator
     * @return : index iterator
     */

    IndexIterator begin() {
        KeyType useless = null;
        BPlusTreeLeafPage start_leaf = findLeafPage(useless, true);
        tryUnlockRootPageID(false);
        return new IndexIterator(start_leaf, 0, buffer_pool_manager);
    }

    IndexIterator begin(KeyType key) {
        BPlusTreeLeafPage startLeaf = findLeafPage(key);
        tryUnlockRootPageID(false);
        if (startLeaf == null) {
            return new IndexIterator(startLeaf, 0, buffer_pool_manager);
        }
        int idx = startLeaf.keyIndex(key,comparator);
        return new IndexIterator(startLeaf, idx, buffer_pool_manager);
    }

    /*****************************************************************************
     * UTILITIES AND DEBUG
     *****************************************************************************/
    /*
     * Find leaf page containing particular key, if leftMost flag == true, find
     * the left most leaf page
     */

    BPlusTreeLeafPage findLeafPage(KeyType key) throws Exception{
        return this.findLeafPage(key, false, READ, null);
    }

    BPlusTreeLeafPage findLeafPage(KeyType key, boolean leftMost) throws Exception {
        return this.findLeafPage(key, leftMost, READ, null);
    }

    BPlusTreeLeafPage findLeafPage(KeyType key, boolean leftMost, OpType op,
                                                             Transaction transaction) throws Exception {
        boolean exclusive = (op != READ);
        lockRootPageID(exclusive);
        if (isEmpty()) {
            tryUnlockRootPageID(exclusive);
            return null;
        }
        //, you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either
        // a leaf or an internal page, and unpin the page after any writing or reading operations.
        BPlusTreePage pointer =  crabingProtocalFetchPage(rootPageID,op,-1,transaction);
        int next;
        for (int cur = rootPageID;
             !pointer.isLeafPage();
             pointer = (BPlusTreeLeafPage) crabingProtocalFetchPage(next,op,cur,transaction),cur = next) {
            BPlusTreeInternalPage internalPage = (BPlusTreeInternalPage) pointer;
            if (leftMost) {
                next = (int) internalPage.valueAt(0);
            }else {
                next = (int) internalPage.lookup(key,comparator);
            }
        }
        return (BPlusTreeLeafPage) pointer;
    }

    private BPlusTreePage fetchPage(int page_id) {
        Page page = buffer_pool_manager.fetchPage(page_id);
        return new BPlusTreePage(page);
    }


    private BPlusTreePage crabingProtocalFetchPage(int page_id, OpType op, int previous, Transaction transaction) throws Exception {
        boolean exclusive = op != READ;
        Page page = buffer_pool_manager.fetchPage(page_id);
        lock(exclusive,page);
        BPlusTreePage treePage = new BPlusTreePage(page);
        if (previous > 0 && (!exclusive || treePage.isSafe(op))) {
            freePagesInTransaction(exclusive,transaction,previous);
        }
        if (transaction != null)
            transaction.addIntoPageSet(page);
        return treePage;
    }

    private void freePagesInTransaction(boolean exclusive, Transaction transaction) throws Exception {
        freePagesInTransaction(exclusive, transaction, -1);
    }


    private void freePagesInTransaction(boolean exclusive, Transaction transaction, int cur) throws Exception {
        tryUnlockRootPageID(exclusive);
        if (transaction == null) {
            if(exclusive || cur < 0)
                throw new Exception("Unable to free pages in Transaction");
            unlock(false,cur);
            buffer_pool_manager.unpinPage(cur,false);
            return;
        }
        for (Page page : transaction.getPageSet()) {
            int curPid = page.getPageID();
            unlock(exclusive,page);
            buffer_pool_manager.unpinPage(curPid,exclusive);
            if (transaction.getDeletedPageSet().find(curPid) != transaction.getDeletedPageSet().end()) {
                buffer_pool_manager.deletePage(curPid);
                transaction.getDeletedPageSet().erase(curPid);
            }
        }
        if(!transaction.getDeletedPageSet().empty())
            throw new Exception("Unable to free pages in Transaction");
        transaction.getPageSet().clear();
    }

    /*
     * Update/Insert root page id in header page(where page_id = 0, header_page is
     * defined under include/page/header_page.h)
     * Call this method everytime root page id is changed.
     * @parameter: insert_record      defualt value is false. When set to true,
     * insert a record <index_name, root_page_id> into header page instead of
     * updating it.
     */
    private void updateRootPageID(){
        updateRootPageID(0);
    }

    private void updateRootPageID(int insert_record) {
        HeaderPage header_page =  buffer_pool_manager.fetchPage(HEADER_PAGE_ID);
        if (insert_record == 1)
            // create a new record<index_name + root_page_id> in header_page
            header_page.insertRecord(index_name, rootPageID);
        else
            // update root_page_id in header_page
            header_page.updateRecord(index_name, rootPageID);
        buffer_pool_manager.unpinPage(HEADER_PAGE_ID, true);
    }

    /***************************************************************************
     *  Check integrity of B+ tree data structure.
     ***************************************************************************/


    public int isBalanced(int pid) throws Exception {
        if (isEmpty()) return 1;
        BPlusTreePage node = buffer_pool_manager.fetchPage(pid);
        if (node == null) {
            throw new Exception("all page are pinned while isBalanced");
        }
        int ret = 0;
        if (!node.isLeafPage())  {
            BPlusTreeInternalPage page = (BPlusTreeInternalPage) node;
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
    }


    public boolean isPageCorr(int pid, Pair<KeyType,KeyType> out) throws Exception {
        if (isEmpty()) return true;
        BPlusTreePage node = buffer_pool_manager.fetchPage(pid);
        if (node == null) {
            throw new Exception("all page are pinned while isPageCorr");
        }
        boolean ret = true;
        if (node.isLeafPage())  {
            BPlusTreeLeafPage page = (BPlusTreeLeafPage) node;
            int size = page.getSize();
            ret = ret && (size >= node.getMinSize() && size <= node.getMaxSize());
            for (int i = 1; i < size; i++) {
                if (comparator.compare(page.keyAt(i-1), page.keyAt(i)) > 0) {
                    ret = false;
                    break;
                }
            }
            out = new Pair<KeyType,KeyType>((KeyType)page.keyAt(0),(KeyType)page.keyAt(size-1));
        } else {
            BPlusTreeInternalPage page = (BPlusTreeInternalPage) node;
            int size = page.getSize();
            ret = ret && (size >= node.getMinSize() && size <= node.getMaxSize());
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
    }


    public boolean check(boolean forceCheck) throws Exception {
        if (!forceCheck && !openCheck) {
            return true;
        }
        Pair<KeyType,KeyType> in = new Pair<>();
        boolean isPageInOrderAndSizeCorr = isPageCorr(rootPageID, in);
        boolean isBal = isBalanced(rootPageID) != 0;
        boolean isAllUnpin = buffer_pool_manager.checkAllUnpined();
        if (!isPageInOrderAndSizeCorr) System.out.println("problem in page order or page size");
        if (!isBal) System.out.println("problem in balance");
        if (!isAllUnpin) System.out.println("problem in page unpin");
        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
    }


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
        Page page = buffer_pool_manager.fetchPage(pageID);
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
