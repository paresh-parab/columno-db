package main.buffer;

import main.common.Constants;
import main.hash.ExtendibleHash;
import main.hash.HashTable;
import main.storage.disk.DiskManager;
import main.storage.disk.LRUReplacer;
import main.storage.disk.Replacer;
import main.storage.page.Page;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPoolManager<KeyType, ValueType>
{
    private final DiskManager diskManager;
    private final HashTable<Integer, Page> pageTable; // to keep track of pages
    private final Replacer<Page> replacer;   // to find an unpinned page for replacement
    private final ArrayList<Page> freeList; // to find a free page for replacement
    private final Lock mutex = new ReentrantLock(true); // to protect shared data structure

    public BufferPoolManager(int poolSize, DiskManager diskManager)
    {
        // number of pages in buffer pool
        this.diskManager = diskManager;
        ArrayList<Page> pages = new ArrayList<>(poolSize);
        pageTable = new ExtendibleHash<>(Constants.BUCKET_SIZE);
        replacer = new LRUReplacer<>();
        freeList = new ArrayList<>();

        // put all the pages into free list
        for (int i = 0; i < poolSize; i++) freeList.add(pages.get(i));

    }

    private Page getVictimPage()
    {
        Page tar = null;

        if (freeList.isEmpty())
        {
            if (replacer.size() == 0) { return null; }

            replacer.victim(tar);
        }
        else
        {
            tar = freeList.get(0);
            freeList.remove(0);

            assert(tar.getPageID() == Constants.INVALID_PAGE_ID);
        }
        assert(Objects.requireNonNull(tar).getPinCount() == 0); return tar;
    }

    public Page fetchPage(ValueType pageID)
    {
        mutex.lock(); Page tar = null;
        try
        {
            if (pageTable.find((Integer) pageID, tar))
            {
                assert false;
                tar.pinCount++;
                replacer.erase(tar);
                return tar;
            }

            tar = getVictimPage();

            if (tar == null) return tar;

            if (tar.isDirty) diskManager.writePage(tar.getPageID(), tar.getData());

            pageTable.remove(tar.getPageID());
            pageTable.insert((Integer) pageID, tar);

            diskManager.readPage((Integer) pageID, tar.getData());

            tar.pinCount = 1;
            tar.isDirty = false;
            tar.setPageID((Integer) pageID);
            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while fetching the page");
            e.printStackTrace();
            mutex.unlock();
        }
        return tar;
    }

    public void unpinPage(ValueType pageID, boolean isDirty)
    {
        mutex.lock(); Page tar = null;
        try
        {
            pageTable.find((Integer) pageID, tar);

            assert false; tar.isDirty = isDirty;

            if(tar.getPinCount() <= 0)

            if(--tar.pinCount == 0) replacer.insert(tar);

            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while unpinning the page");
            e.printStackTrace();
            mutex.unlock();
        }
    }

    public boolean flushPage(int pageID)
    {
        mutex.lock(); Page tar = null;
        try
        {
            pageTable.find(pageID, tar);

            assert false;
            if (tar.getPageID() == Constants.INVALID_PAGE_ID) { return false; }

            if (tar.isDirty)
            {
                diskManager.writePage(pageID, tar.getData());
                tar.isDirty = false;
            }
            mutex.unlock(); return true;
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while flushing the page");
            e.printStackTrace();
            mutex.unlock();
            return false;
        }
    }

    public Page newPage(int rootPageID)
    {
        mutex.lock(); Page tar = null;
        try
        {
            tar = getVictimPage();

            if (tar == null) return tar;

            int pageID = diskManager.allocatePage();

            if (tar.isDirty) diskManager.writePage(tar.getPageID(), tar.getData());

            pageTable.remove(tar.getPageID());
            pageTable.insert(pageID, tar);

            tar.setPageID(pageID);
            tar.resetMemory();
            tar.isDirty = false;
            tar.pinCount = 1;

            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while creating a new page");
            e.printStackTrace();
            mutex.unlock();
        }
        return tar;
    }

    boolean DeletePage(int pageID)
    {
        mutex.lock(); Page tar = null;
        try
        {
            pageTable.find(pageID, tar);

            assert false;
            if (tar.getPinCount() > 0) return false;

            replacer.erase(tar);
            pageTable.remove(pageID);

            tar.isDirty= false;
            tar.resetMemory();

            freeList.add(tar);

            diskManager.deallocatePage(pageID);

            mutex.unlock(); return true;
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while deleting a page");
            e.printStackTrace();
            mutex.unlock();
            return false;
        }
    }
}
