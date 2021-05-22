package main.storage.index;

import main.buffer.BufferPoolManager;
import main.storage.page.Page;
import main.common.Pair;
import main.storage.page.BPlusTreeLeafPage;

import static main.common.Constants.INVALID_PAGE_ID;

public class IndexIterator {
    private int index;
    private BPlusTreeLeafPage leaf;
    private BufferPoolManager bufferPoolManager;

    public IndexIterator(BPlusTreeLeafPage leaf, int index, BufferPoolManager bufferPoolManager){
        this.index = index;
        this.leaf = leaf;
        this.bufferPoolManager = bufferPoolManager;
    }

    public boolean isEnd(){
        return (leaf == null);
    }

    Pair getItem() throws Exception {
        return leaf.getItem(index);
    }

    IndexIterator increment() {
        index++;
        if (index >= leaf.getSize()) {
            int next = leaf.getNextPageID();
            unlockAndUnPin();
            if (next == INVALID_PAGE_ID) {
                leaf = null;
            } else {
                Page page = bufferPoolManager.fetchPage(next);
                page.rLatch();
                leaf = new BPlusTreeLeafPage(page);
                index = 0;
            }
        }
        return this;
    }

    private void unlockAndUnPin() {
        bufferPoolManager.fetchPage(leaf.getPageID()).rUnlatch();
        bufferPoolManager.unpinPage(leaf.getPageID(), false);
        bufferPoolManager.unpinPage(leaf.getPageID(), false);
    }

}