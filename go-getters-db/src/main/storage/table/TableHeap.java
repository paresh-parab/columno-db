package main.storage.table;

import main.buffer.BufferPoolManager;
import main.catalog.Schema;
import main.common.Pair;
import main.storage.page.TablePage;
import main.type.PageType;
import main.type.TypeID;

import java.util.ArrayList;
import java.util.List;

import static main.common.Constants.*;

public class TableHeap {

    private BufferPoolManager bpm;
    private int firstPageID;

    public TableHeap(BufferPoolManager bpm, Schema schema) {
        DEBUGGER.info("Initializing Table with schema and Buffer pool manager");
        this.bpm = bpm;
        DEBUGGER.info("Reserving first page for the table");
        TablePage firstPage = (TablePage) bpm.newPage(PageType.TABLE);
        this.firstPageID = firstPage.getPageID();
        firstPage.setSchema(schema);
        bpm.unpinPage(firstPageID, true);
    }

    private boolean insertTupleIntoPage(TablePage page, Tuple t){

        List<Tuple> data = page.getData();
        if(data.size() == PAGE_SIZE){
            DEBUGGER.info("Current page found to be full !");
            return false;
        }
        data.add(t);
        DEBUGGER.info("Tuple was succesfully inserted in current page! ");
        return true;
    }

    public boolean insertTuple(Tuple tuple) {
        TablePage currentPage = (TablePage) bpm.fetchPage(firstPageID);
        if (currentPage == null) {
            //Page doesnt exist
            return false;
        }

        currentPage.wLatch();

        // Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
        // INVARIANT: cur_page is WLatched if you leave the loop normally.
        DEBUGGER.info("Attempting to insert into current page of Table");
        while (!insertTupleIntoPage(currentPage, tuple)) {

            int nextPageID = currentPage.getNextPageID();
            // If the next page is a valid page,
            if (nextPageID != INVALID_PAGE_ID) {
                // Unlatch and unpin the current page.
                currentPage.wUnlatch();
                bpm.unpinPage(currentPage.getPageID(), false);
                // And repeat the process with the next page.
                currentPage = (TablePage) bpm.fetchPage(nextPageID);
                DEBUGGER.info("Reading in next page of table");
                currentPage.wLatch();
            } else {
                DEBUGGER.info("We ran out of pages; Creating empty new page");
                // Otherwise we have run out of valid pages. We need to create a new page.
                TablePage newPage = (TablePage) bpm.newPage(PageType.TABLE);
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

    public List getColumnValues(int colIdx, TypeID type) {
        List result = null;

        if(type == TypeID.STRING_TYPE){
            result = new ArrayList<Pair<String, Integer>>();
        }else{
            result = new ArrayList<Pair<String, Integer>>();
        }

        int nextPageID = firstPageID;

        do {
            TablePage currentPage = (TablePage) bpm.fetchPage(nextPageID);
            DEBUGGER.info("Reading next page of table");
            if (currentPage == null) {
                break;
            }
            nextPageID = currentPage.getNextPageID();
            currentPage.wLatch();

            int currentPageID = currentPage.getPageID();
            List<Tuple> currentTuples = currentPage.getData();
            DEBUGGER.info("Loading target column values from current page");
            for(int i=0; i < currentTuples.size(); i++){

                if(type == TypeID.STRING_TYPE){
                    Pair<String, Integer> pair = new Pair<String, Integer>(currentTuples.get(i).getValue(colIdx).getAsString(),
                            currentPageID + i);
                    result.add(pair);

                }else{
                    Pair<Integer, Integer> pair = new Pair<Integer, Integer>(currentTuples.get(i).getValue(colIdx).getAsInteger(),
                            currentPageID + i);
                    result.add(pair);
                }

            }

            currentPage.wUnlatch();

        }while( nextPageID != INVALID_PAGE_ID);
        return result;
    }

    public List readAllRows() {

        List result = new ArrayList<Tuple>();
        int nextPageID = firstPageID;

        do {
            TablePage currentPage = (TablePage) bpm.fetchPage(nextPageID);
            DEBUGGER.info("Reading in next page");

            if (currentPage == null) {
                break;
            }
            nextPageID = currentPage.getNextPageID();
            currentPage.wLatch();

            int currentPageID = currentPage.getPageID();
            List<Tuple> currentTuples = currentPage.getData();
            DEBUGGER.info("Loading all rows from current page");

            result.addAll(new ArrayList(currentTuples));
            currentPage.wUnlatch();

        }while( nextPageID != INVALID_PAGE_ID);
        return result;
    }


}
