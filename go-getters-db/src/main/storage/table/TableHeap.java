package main.storage.table;

import main.buffer.BufferPoolManager;
import main.storage.page.Page;

import java.util.List;

import static main.common.Constants.INVALID_PAGE_ID;
import static main.common.Constants.PAGE_SIZE;

public class TableHeap {

    private BufferPoolManager bpm;
    private int firstPageID;

    public TableHeap(BufferPoolManager bpm) {
        this.bpm = bpm;
        Page<Tuple> firstPage = bpm.newPage(firstPageID);
        this.firstPageID = firstPage.getPageID();
        bpm.unpinPage(firstPageID, true);
    }

    private boolean insertTupleIntoPage(Page<Tuple> page, Tuple t){
        List<Tuple> data = page.getData();
        if(data.size() == PAGE_SIZE){
            return false;
        }
        data.add(t);
        return true;
    }

    public boolean insertTuple(Tuple tuple) {
        Page<Tuple> currentPage = bpm.fetchPage(firstPageID);
        if (currentPage == null) {
            //Page doesnt exist
            return false;
        }

        currentPage.wLatch();
        // Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
        // INVARIANT: cur_page is WLatched if you leave the loop normally.
        while (!insertTupleIntoPage(currentPage, tuple)) {
            int nextPageID = currentPage.getNextPageID();
            // If the next page is a valid page,
            if (nextPageID != INVALID_PAGE_ID) {
                // Unlatch and unpin the current page.
                currentPage.wUnlatch();
                bpm.unpinPage(currentPage.getPageID(), false);
                // And repeat the process with the next page.
                currentPage = bpm.fetchPage(nextPageID);
                currentPage.wLatch();
            } else {
                // Otherwise we have run out of valid pages. We need to create a new page.
                Page<Tuple> newPage = bpm.newPage(nextPageID);
                // If we could not create a new page,
                if (newPage == null) {
                    // Then life sucks and we abort the transaction.
                    currentPage.wUnlatch();
                    bpm.unpinPage(currentPage.getPageID(), false);
                    return false;
                }
                // Otherwise we were able to create a new page. We initialize it now.
                newPage.wLatch();
                currentPage.setNextPageID(nextPageID);
                newPage.setPageID(nextPageID);
                currentPage.wUnlatch();
                bpm.unpinPage(currentPage.getPageID(), true);
                currentPage = newPage;
            }
        }
        // This line has caused most of us to double-take and "whoa double unlatch".
        // We are not, in fact, double unlatching. See the invariant above.
        currentPage.wUnlatch();
        bpm.unpinPage(currentPage.getPageID(), true);
        // Update the transaction's write set.
        return true;
    }


}
