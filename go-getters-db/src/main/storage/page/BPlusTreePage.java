package main.storage;

import java.util.List;

import static main.storage.Constants.INVALID_PAGE_ID;

public class BPlusTreePage<KeyType, ValueType> {

    //Page Type (internal or leaf)
    private IndexPageType pageType;
    //Log sequence number
    private int logSeqNum;
    //Number of Key & Value pairs in page
    private int size;
    //Max number of Key & Value pairs in page
    private int maxSize;
    //Parent Page Id
    private int parentPageID;
    //self page Id
    private int pageID;

    List<Pair<KeyType, ValueType>> array ;

    public BPlusTreePage(Page<KeyType, ValueType> page){
        setPageID(page.getPageID());
        array = page.getData();
    }

    public IndexPageType getPageType() {
        return pageType;
    }

    public void setPageType(IndexPageType indexPageType) {
        this.pageType = indexPageType;
    }

    public int getLogSeqNum() {
        return logSeqNum;
    }

    public void setLogSeqNum(int logSeqNum) {
        this.logSeqNum = logSeqNum;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getParentPageID() {
        return parentPageID;
    }

    public void setParentPageID(int parentPageID) {
        this.parentPageID = parentPageID;
    }

    public int getPageID() {
        return pageID;
    }

    public void setPageID(int pageID) {
        this.pageID = pageID;
    }

    public boolean isRootPage(){
        return parentPageID == INVALID_PAGE_ID;
    }

    public boolean isLeafPage(){
        return pageType == IndexPageType.LEAF_PAGE;
    }

    public void increaseSize(int amt){
        size += amt;
    }

    public int getMinSize(){
        //root that is leaf can be minimum 1
        //root that is not leaf will have minimum 2 children
        //max size is ceil(m/2)
        return isRootPage() ? (isLeafPage() ? 1: 2) : ((maxSize+1)/2);
    }

    public <N extends BPlusTreePage> void moveFirstToEndOf(N recipient, BufferPoolManager bpm) {
    }

    public <N extends BPlusTreePage> void moveLastToFrontOf(N recipient, int parentIndex, BufferPoolManager bpm) {
    }
}
