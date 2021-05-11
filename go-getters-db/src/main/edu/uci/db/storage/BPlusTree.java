package main.edu.uci.db.storage;

import java.util.Comparator;

public class BPlusTree <KeyType, ValueType, KeyComparator extends Comparator>{
//
//
////    Mutex mutex ;
////    static thread_local int rootLockedCnt;
//
//    private String index_name;
//    private KeyComparator comparator;
//    private BufferPoolManager buffer_pool_manager;
//    private int root_page_id;
//
//    public BPlusTree(String name, BufferPoolManager buffer_pool_manager, KeyComparator comparator, int root_page_id) {
//        this.index_name = name;
//        this.buffer_pool_manager = buffer_pool_manager;
//        this.comparator = comparator;
//        this.root_page_id = root_page_id;
//
//    }
//
//    public BPlusTree(String name, BufferPoolManager buffer_pool_manager, KeyComparator comparator) {
//        this(name, buffer_pool_manager, comparator, INVALID_PAGE_ID);
//    }
//
//    // Returns true if this B+ tree has no keys and values.
//    boolean isEmpty() {
//        return root_page_id == INVALID_PAGE_ID;
//    };
//
//    // return the value associated with a given key
//    boolean getValue(KeyType key, List<ValueType> result) {
//        return this.getValue(key, result, null);
//    }
//
//    boolean getValue(KeyType key, List<ValueType> result, Transaction transaction) {
//        //step 1. find page
//        BPlusTreeLeafPage tar = findLeafPage(key,false, OpType::READ,transaction);
//        if (tar == null)
//            return false;
//        //step 2. find value
//        //result.resize(1);
//        boolean ret = tar.lookup(key, result.get(0), comparator );
//        //step 3. unPin buffer pool
//        freePagesInTransaction(false,transaction,tar.getPageId());
//        //buffer_pool_manager_->UnpinPage(tar->GetPageId(), false);
//        //assert(buffer_pool_manager_->CheckAllUnpined());
//        return ret;
//    }
//
//    // Insert a key-value pair into this B+ tree.
//    boolean insert(KeyType key, ValueType value) {
//        return this.insert(key, value, null);
//    }
//
//    boolean insert(KeyType key, ValueType value, Transaction transaction) throws Exception {
//        lockRootPageId(true);
//        if (isEmpty()) {
//            startNewTree(key,value);
//            tryUnlockRootPageID(true);
//            return true;
//        }
//        tryUnlockRootPageID(true);
//        boolean res = insertIntoLeaf(key,value,transaction);
//        //assert(Check());
//        return res;
//    }
//
//    void StartNewTree(KeyType key, ValueType value) {
//        //step 1. ask for new page from buffer pool manager
//        int newPageId;
//        Page rootPage = buffer_pool_manager.newPage(newPageId);
//        //assert(rootPage != nullptr);
//
//        BPlusTreeLeafPage root = rootPage.getData();
//
//        //step 2. update b+ tree's root page id
//        root.init(newPageId, INVALID_PAGE_ID);
//        root_page_id = newPageId;
//        updateRootPageId(true);
//        //step 3. insert entry directly into leaf page.
//        root.insert(key,value,comparator);
//
//        buffer_pool_manager.unpinPage(newPageId,true);
//    }
//
//    /*
//     * Insert constant key & value pair into leaf page
//     * User needs to first find the right leaf page as insertion target, then look
//     * through leaf page to see whether insert key exist or not. If exist, return
//     * immdiately, otherwise insert entry. Remember to deal with split if necessary.
//     * @return: since we only support unique key, if user try to insert duplicate
//     * keys return false, otherwise return true.
//     */
//    boolean insertIntoLeaf(KeyType key, ValueType value) throws Exception {
//        return this.insertIntoLeaf(key, value, null);
//    }
//
//    boolean insertIntoLeaf(KeyType key, ValueType value, Transaction transaction) throws Exception {
//        BPlusTreeLeafPage leafPage = findLeafPage(key,false,OpType::INSERT,transaction);
//        ValueType v;
//        boolean exist = leafPage.lookup(key,v,comparator);
//        if (exist) {
//            //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), false);
//            freePagesInTransaction(true,transaction);
//            return false;
//        }
//        leafPage.insert(key,value,comparator_);
//        if (leafPage.getSize() > leafPage.getMaxSize()) {//insert then split
//            BPlusTreeLeafPage newLeafPage = split(leafPage,transaction);//unpin it in below func
//            InsertIntoParent(leafPage,newLeafPage.keyAt(0),newLeafPage,transaction);
//        }
//        //buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
//        freePagesInTransaction(true,transaction);
//        return true;
//    }
//
//    /*
//     * Split input page and return newly created page.
//     * Using template N to represent either internal page or leaf page.
//     * User needs to first ask for new page from buffer pool manager(NOTICE: throw
//     * an "out of memory" exception if returned value is nullptr), then move half
//     * of key & value pairs from input page to newly created page
//     */
//
//    public <N> N split(N node, Transaction transaction) {
//        //step 1 ask for new page from buffer pool manager
//        int newPageId;
//        Page newPage = buffer_pool_manager.newPage(newPageId);
//        //assert(newPage != nullptr);
//        newPage.wLatch();
//        transaction.addIntoPageSet(newPage);
//        //step 2 move half of key & value pairs from input page to newly created page
//        N newNode = newPage.getData();
//        newNode.init(newPageId, node.getParentPageId());
//        node.moveHalfTo(newNode, buffer_pool_manager);
//        //fetch page and new page need to unpin page(do it outside)
//        return newNode;
//    }
//
//    /*
//     * Insert key & value pair into internal page after split
//     * @param   old_node      input page from split() method
//     * @param   key
//     * @param   new_node      returned page from split() method
//     * User needs to first find the parent page of old_node, parent node must be
//     * adjusted to take info of new_node into account. Remember to deal with split
//     * recursively if necessary.
//     */
//
//    public void insertIntoParent(BPlusTreePage old_node, KeyType key, BPlusTreePage new_node, Transaction transaction) throws Exception {
//        if (old_node.isRootPage()) {
//            Page newPage = buffer_pool_manager.newPage(root_page_id);
//            //assert(newPage != nullptr);
//            //assert(newPage->GetPinCount() == 1);
//            BPlusTreeInternalPage newRoot = newPage.getData();
//            newRoot.init(root_page_id);
//            newRoot.populateNewRoot(old_node.getPageID(),key,new_node.getPageID());
//            old_node.setParentPageID(root_page_id);
//            new_node.setParentPageID(root_page_id);
//            updateRootPageID();
//            //fetch page and new page need to unpin page
//            //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
//            buffer_pool_manager.unpinPage(newRoot.getPageID(),true);
//            return;
//        }
//        int parentId = old_node.getParentPageID();
//        BPlusTreePage page = fetchPage(parentId);
//        //assert(page != nullptr);
//        BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page;
//        new_node.setParentPageID(parentId);
//        //buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
//        //insert new node after old node
//        parent.insertNodeAfter(old_node.getPageID(), key, new_node.getPageID());
//        if (parent.getSize() > parent.getMaxSize()) {
//            //begin /* Split Parent */
//            BPlusTreeInternalPage newLeafPage = split(parent,transaction);//new page need unpin
//            insertIntoParent(parent, (KeyType) newLeafPage.keyAt(0),newLeafPage,transaction);
//        }
//        buffer_pool_manager.unpinPage(parentId,true);
//    }
//
//
//    /*****************************************************************************
//     * REMOVE
//     *****************************************************************************/
//    /*
//     * Delete key & value pair associated with input key
//     * If current tree is empty, return immdiately.
//     * If not, User needs to first find the right leaf page as deletion target, then
//     * delete entry from leaf page. Remember to deal with redistribute or merge if
//     * necessary.
//     */
//
//    public void Remove(KeyType key) throws Exception {
//        remove(key, null);
//    }
//    void remove(KeyType key, Transaction transaction) throws Exception {
//        if (isEmpty()) return;
//        BPlusTreeLeafPage delTar = findLeafPage(key,false,OpType::DELETE,transaction);
//        int curSize = delTar.removeAndDeleteRecord(key,comparator);
//        if (curSize < delTar.getMinSize()) {
//            coalesceOrRedistribute(delTar,transaction);
//        }
//        FreePagesInTransaction(true,transaction);
//        //assert(Check());
//    }
//
//    /*
//     * User needs to first find the sibling of input page. If sibling's size + input
//     * page's size > page's max size, then redistribute. Otherwise, merge.
//     * Using template N to represent either internal page or leaf page.
//     * @return: true means target leaf page should be deleted, false means no
//     * deletion happens
//     */
//
//    public <N extends BPlusTreePage> boolean coalesceOrRedistribute(N node, Transaction transaction) {
//        //if (N is the root and N has only one remaining child)
//        if (node.isRootPage()) {
//            boolean delOldRoot = adjustRoot(node);//make the child of N the new root of the tree and delete N
//            if (delOldRoot) {transaction.addIntoDeletedPageSet(node.getPageID());}
//            return delOldRoot;
//        }
//        //Let N2 be the previous or next child of parent(N)
//        N node2;
//        boolean isRightSib = findLeftSibling(node,node2,transaction);
//        BPlusTreePage parent = fetchPage(node.getParentPageID());
//        BPlusTreeInternalPage parentPage = (BPlusTreeInternalPage) parent;
//        //if (entries in N and N2 can fit in a single node)
//        if (node.getSize() + node2.getSize() <= node.getMaxSize()) {
//            if (isRightSib) {
//                //swap(
//                N temp = node;
//                node = node2;
//                node2= temp;
//                //assumption node is after node2
//            }
//
//            int removeIndex = parentPage.valueIndex(node.getPageID());
//            coalesce(node2,node,parentPage,removeIndex,transaction);//unpin node,node2
//            buffer_pool_manager.unpinPage(parentPage.getPageID(), true);
//            return true;
//        }
//        /* Redistribution: borrow an entry from N2 */
//        int nodeInParentIndex = parentPage.valueIndex(node.getPageID());
//        redistribute(node2,node,nodeInParentIndex);//unpin node,node2
//        buffer_pool_manager.unpinPage(parentPage.getPageID(), false);
//        return false;
//    }
//
//
//    public <N extends BPlusTreePage> boolean findLeftSibling(N node, N sibling, Transaction transaction) {
//        BPlusTreePage page = fetchPage(node.getParentPageID());
//        BPlusTreeInternalPage parent = (BPlusTreeInternalPage) page;
//        int index = parent.valueIndex(node.getPageID());
//        int siblingIndex = index - 1;
//        if (index == 0) { //no left sibling
//            siblingIndex = index + 1;
//        }
//        sibling = crabingProtocalFetchPage( parent.valueAt(siblingIndex),OpType::DELETE,-1,transaction);
//        buffer_pool_manager.unpinPage(parent.getPageID(), false);
//        return index == 0;//index == 0 means sibling is right
//    }
//
//    /*
//     * Move all the key & value pairs from one page to its sibling page, and notify
//     * buffer pool manager to delete this page. Parent page must be adjusted to
//     * take info of deletion into account. Remember to deal with coalesce or
//     * redistribute recursively if necessary.
//     * Using template N to represent either internal page or leaf page.
//     * @param   neighbor_node      sibling page of input "node"
//     * @param   node               input from method coalesceOrRedistribute()
//     * @param   parent             parent page of input "node"
//     * @return  true means parent node should be deleted, false means no deletion
//     * happend
//     */
//    public  <N extends BPlusTreePage> boolean coalesce( N neighbor_node, N node, BPlusTreeInternalPage<KeyType,Integer, KeyComparator> parent,
//                                               int index, Transaction transaction) {
//        //assumption neighbor_node is before node
//        assert(node.getSize() + neighbor_node.getSize() <= node.getMaxSize());
//        //move later one to previous one
//        node.moveAllTo(neighbor_node,index,buffer_pool_manager);
//        transaction.addIntoDeletedPageSet(node.getPageID());
//        parent.remove(index);
//        if (parent.getSize() <= parent.getMinSize()) {
//            return coalesceOrRedistribute(parent,transaction);
//        }
//        return false;
//    }
//
//
//    /*
//     * Redistribute key & value pairs from one page to its sibling page. If index ==
//     * 0, move sibling page's first key & value pair into end of input "node",
//     * otherwise move sibling page's last key & value pair into head of input
//     * "node".
//     * Using template N to represent either internal page or leaf page.
//     * @param   neighbor_node      sibling page of input "node"
//     * @param   node               input from method coalesceOrRedistribute()
//     */
//    public <N extends BPlusTreePage> void redistribute(N neighbor_node, N node, int index) {
//        if (index == 0) {
//            neighbor_node.moveFirstToEndOf(node,buffer_pool_manager);
//        } else {
//            neighbor_node.moveLastToFrontOf(node, index, buffer_pool_manager);
//        }
//    }
//
//    /*
//     * Update root page if necessary
//     * NOTE: size of root page can be less than min size and this method is only
//     * called within coalesceOrRedistribute() method
//     * case 1: when you delete the last element in root page, but root page still
//     * has one last child
//     * case 2: when you delete the last element in whole b+ tree
//     * @return : true means root page should be deleted, false means no deletion
//     * happend
//     */
//    public boolean adjustRoot(BPlusTreePage old_root_node) throws Exception {
//        if (old_root_node.isLeafPage()) {// case 2
//            assert(old_root_node.getSize() == 0);
//            assert (old_root_node.getParentPageID() == INVALID_PAGE_ID);
//            root_page_id = INVALID_PAGE_ID;
//            updateRootPageID();
//            return true;
//        }
//        if (old_root_node.getSize() == 1) {// case 1
//            BPlusTreeInternalPage root = (BPlusTreeInternalPage) old_root_node;
//            int newRootId = (int) root.removeAndReturnOnlyChild();
//            root_page_id = newRootId;
//            updateRootPageID();
//            // set the new root's parent id "INVALID_PAGE_ID"
//            Page page = buffer_pool_manager.fetchPage(newRootId);
//            //assert(page != nullptr);
//            BPlusTreeInternalPage newRoot =
//                    (page.getData());
//            newRoot.setParentPageID(INVALID_PAGE_ID);
//            buffer_pool_manager.unpinPage(newRootId, true);
//            return true;
//        }
//        return false;
//    }
//
//    /*****************************************************************************
//     * INDEX ITERATOR
//     *****************************************************************************/
//    /*
//     * Input parameter is void, find the leaftmost leaf page first, then construct
//     * index iterator
//     * @return : index iterator
//     */
//
//    IndexIterator begin() {
//        KeyType useless = null;
//        BPlusTreeLeafPage start_leaf = findLeafPage(useless, true);
//        tryUnlockRootPageID(false);
//        return new IndexIterator(start_leaf, 0, buffer_pool_manager);
//    }
//
//    IndexIterator begin(KeyType key) {
//        BPlusTreeLeafPage start_leaf = findLeafPage(key);
//        tryUnlockRootPageID(false);
//        if (start_leaf == null) {
//            return new IndexIterator(start_leaf, 0, buffer_pool_manager);
//        }
//        int idx = start_leaf.keyIndex(key,comparator);
//        return new IndexIterator(start_leaf, idx, buffer_pool_manager);
//    }
//
//    /*****************************************************************************
//     * UTILITIES AND DEBUG
//     *****************************************************************************/
//    /*
//     * Find leaf page containing particular key, if leftMost flag == true, find
//     * the left most leaf page
//     */
//
//    BPlusTreeLeafPage findLeafPage(KeyType key, boolean leftMost, OpType op,
//                                                             Transaction transaction) throws Exception {
//        boolean exclusive = (op != OpType::READ);
//        LockRootPageId(exclusive);
//        if (isEmpty()) {
//            TryUnlockRootPageId(exclusive);
//            return null;
//        }
//        //, you need to first fetch the page from buffer pool using its unique page_id, then reinterpret cast to either
//        // a leaf or an internal page, and unpin the page after any writing or reading operations.
//        BPlusTreeLeafPage pointer = crabingProtocalFetchPage(root_page_id,op,-1,transaction);
//        int next;
//        for (int cur = root_page_id; !pointer.isLeafPage(); pointer =
//                crabingProtocalFetchPage(next,op,cur,transaction),cur = next) {
//            BPlusTreeInternalPage internalPage = (BPlusTreeInternalPage) pointer;
//            if (leftMost) {
//                next = (int) internalPage.valueAt(0);
//            }else {
//                next = (int) internalPage.lookup(key,comparator);
//            }
//        }
//        return pointer;
//    }
//
//    BPlusTreePage fetchPage(int page_id) {
//        BPlusTreePage page = buffer_pool_manager.fetchPage(page_id);
//        return page.getData();
//    }
//
//
//    BPlusTreePage crabingProtocalFetchPage(int page_id, OpType op, int previous, Transaction transaction) {
//        boolean exclusive = op != OpType::READ;
//        BPlusTreePage page = buffer_pool_manager.fetchPage(page_id);
//        lock(exclusive,page);
//        BPlusTreePage treePage = page.getData();
//        if (previous > 0 && (!exclusive || treePage.isSafe(op))) {
//            FreePagesInTransaction(exclusive,transaction,previous);
//        }
//        if (transaction != null)
//            transaction.addIntoPageSet(page);
//        return treePage;
//    }
//
//
//    void freePagesInTransaction(boolean exclusive, Transaction transaction, int cur) {
//        tryUnlockRootPageId(exclusive);
//        if (transaction == null) {
//            assert(!exclusive && cur >= 0);
//            unlock(false,cur);
//            buffer_pool_manager.unpinPage(cur,false);
//            return;
//        }
//        for (Page page : transaction.getPageSet()) {
//            int curPid = page.getPageID();
//            unlock(exclusive,page);
//            buffer_pool_manager.unpinPage(curPid,exclusive);
//            if (transaction.getDeletedPageSet().find(curPid) != transaction.getDeletedPageSet().end()) {
//                buffer_pool_manager.deletePage(curPid);
//                transaction.getDeletedPageSet().erase(curPid);
//            }
//        }
//        assert(transaction.getDeletedPageSet().empty());
//        transaction.getPageSet().clear();
//    }
//
//    /*
//     * Update/Insert root page id in header page(where page_id = 0, header_page is
//     * defined under include/page/header_page.h)
//     * Call this method everytime root page id is changed.
//     * @parameter: insert_record      defualt value is false. When set to true,
//     * insert a record <index_name, root_page_id> into header page instead of
//     * updating it.
//     */
//
//    public void updateRootPageId(int insert_record) {
//        HeaderPage header_page =  buffer_pool_manager.fetchPage(HEADER_PAGE_ID);
//        if (insert_record == 1)
//            // create a new record<index_name + root_page_id> in header_page
//            header_page.insertRecord(index_name, root_page_id);
//        else
//            // update root_page_id in header_page
//            header_page.updateRecord(index_name, root_page_id);
//        buffer_pool_manager.unpinPage(HEADER_PAGE_ID, true);
//    }
//
//    /***************************************************************************
//     *  Check integrity of B+ tree data structure.
//     ***************************************************************************/
//
//
//    public boolean isBalanced(int pid) throws Exception {
//        if (isEmpty()) return true;
//        BPlusTreePage node = buffer_pool_manager.fetchPage(pid);
//        if (node == null) {
//            throw new Exception("all page are pinned while isBalanced");
//        }
//        int ret = 0;
//        if (!node.isLeafPage())  {
//            BPlusTreeInternalPage page = (BPlusTreeInternalPage) node;
//            int last = -2;
//            for (int i = 0; i < page.getSize(); i++) {
//                int cur = isBalanced(page.valueAt(i));
//                if (cur >= 0 && last == -2) {
//                    last = cur;
//                    ret = last + 1;
//                }else if (last != cur) {
//                    ret = -1;
//                    break;
//                }
//            }
//        }
//        buffer_pool_manager.unpinPage(pid,false);
//        return ret != 0;
//    }
//
//
//    public boolean isPageCorr(int pid, Pair<KeyType,KeyType> out) throws Exception {
//        if (isEmpty()) return true;
//        BPlusTreePage node = buffer_pool_manager.fetchPage(pid);
//        if (node == null) {
//            throw new Exception("all page are pinned while isPageCorr");
//        }
//        boolean ret = true;
//        if (node.isLeafPage())  {
//            BPlusTreeLeafPage page = (BPlusTreeLeafPage) node;
//            int size = page.getSize();
//            ret = ret && (size >= node.getMinSize() && size <= node.getMaxSize());
//            for (int i = 1; i < size; i++) {
//                if (comparator.compare(page.keyAt(i-1), page.keyAt(i)) > 0) {
//                    ret = false;
//                    break;
//                }
//            }
//            out = new Pair<KeyType,KeyType>((KeyType)page.keyAt(0),(KeyType)page.keyAt(size-1));
//        } else {
//            BPlusTreeInternalPage page = (BPlusTreeInternalPage) node;
//            int size = page.getSize();
//            ret = ret && (size >= node.getMinSize() && size <= node.getMaxSize());
//            Pair<KeyType,KeyType> left =  new Pair<>(),right = new Pair<>();
//            for (int i = 1; i < size; i++) {
//                if (i == 1) {
//                    ret = ret && isPageCorr((Integer) page.valueAt(0),left);
//                }
//                ret = ret && isPageCorr(page.valueAt(i),right);
//                ret = ret && (comparator.compare(page.keyAt(i) ,left.getValue())>0 && comparator.compare(page.keyAt(i), right.getKey())<=0);
//                ret = ret && (i == 1 || comparator.compare(page.keyAt(i-1) , page.keyAt(i)) < 0);
//                if (!ret) break;
//                left = right;
//            }
//            out = new Pair<KeyType,KeyType>((KeyType) page.keyAt(0), (KeyType) page.keyAt(size-1));
//        }
//        buffer_pool_manager.unpinPage(pid,false);
//        return ret;
//    }
//
//
//    public boolean check(boolean forceCheck) throws Exception {
//        if (!forceCheck && !openCheck) {
//            return true;
//        }
//        Pair<KeyType,KeyType> in = new Pair<>();
//        boolean isPageInOrderAndSizeCorr = isPageCorr(root_page_id, in);
//        boolean isBal = isBalanced(root_page_id);
//        boolean isAllUnpin = buffer_pool_manager.checkAllUnpined();
//        if (!isPageInOrderAndSizeCorr) System.out.println("problem in page order or page size");
//        if (!isBal) System.out.println("problem in balance");
//        if (!isAllUnpin) System.out.println("problem in page unpin");
//        return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
//    }
//
//
//
//
//    /*
//     * Input parameter is low key, find the leaf page that contains the input key
//     * first, then construct index iterator
//     * @return : index iterator
//     */
//
///*
//
//    // index iterator
//    INDEXITERATOR_TYPE Begin();
//    INDEXITERATOR_TYPE Begin(const KeyType &key);
//
//    // Print this B+ tree to stdout using a simple command-line
//    std::string ToString(bool verbose = false);
//
//    // read data from file and insert one by one
//    void InsertFromFile(const std::string &file_name,
//                        Transaction *transaction = nullptr);
//
//    // read data from file and remove one by one
//    void RemoveFromFile(const std::string &file_name,
//                        Transaction *transaction = nullptr);
//    // expose for test purpose
//    B_PLUS_TREE_LEAF_PAGE_TYPE *FindLeafPage(const KeyType &key,
//                                             bool leftMost = false,
//                                             OpType op = OpType::READ,
//                                             Transaction *transaction = nullptr);
//    // expose for test purpose
//    bool Check(bool force = false);
//    bool openCheck = true;
//    private:
//    BPlusTreePage *FetchPage(page_id_t page_id);
//
//
//
//
//    void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key,
//                          BPlusTreePage *new_node,
//                          Transaction *transaction = nullptr);
//
//    template <typename N> N *Split(N *node, Transaction *transaction);
//
//    template <typename N>
//    bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);
//
//    template <typename N>
//    bool FindLeftSibling(N *node, N * &sibling, Transaction *transaction);
//
//    template <typename N>
//    bool Coalesce(
//            N *&neighbor_node, N *&node,
//            BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
//            int index, Transaction *transaction = nullptr);
//
//    template <typename N> void Redistribute(N *neighbor_node, N *node, int index);
//
//    bool AdjustRoot(BPlusTreePage *node);
//
//    void UpdateRootPageId(int insert_record = false);
//
//    BPlusTreePage *CrabingProtocalFetchPage(page_id_t page_id,OpType op, page_id_t previous, Transaction *transaction);
//
//    void FreePagesInTransaction(bool exclusive,  Transaction *transaction, page_id_t cur = -1);
//
//    inline void Lock(bool exclusive,Page * page) {
//        if (exclusive) {
//            page->WLatch();
//        } else {
//            page->RLatch();
//        }
//    }
//
//    inline void Unlock(bool exclusive,Page * page) {
//        if (exclusive) {
//            page->WUnlatch();
//        } else {
//            page->RUnlatch();
//        }
//    }
//    inline void Unlock(bool exclusive,page_id_t pageId) {
//        auto page = buffer_pool_manager_->FetchPage(pageId);
//        Unlock(exclusive,page);
//        buffer_pool_manager_->UnpinPage(pageId,exclusive);
//    }
//    inline void LockRootPageId(bool exclusive) {
//        if (exclusive) {
//            mutex_.WLock();
//        } else {
//            mutex_.RLock();
//        }
//        rootLockedCnt++;
//    }
//
//    inline void TryUnlockRootPageId(bool exclusive) {
//        if (rootLockedCnt > 0) {
//            if (exclusive) {
//                mutex_.WUnlock();
//            } else {
//                mutex_.RUnlock();
//            }
//            rootLockedCnt--;
//        }
//    }
//
//
//    int isBalanced(page_id_t pid);
//    bool isPageCorr(page_id_t pid,pair<KeyType,KeyType> &out);
//    // member variable
//    */
//

}

