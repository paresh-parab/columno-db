package main.buffer;

import main.common.Constants;
import main.hash.ExtendibleHash;
import main.hash.HashTable;
import main.storage.disk.DiskManager;
import main.storage.page.Page;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BufferPoolManager<T>
{
    private final DiskManager<Page<T>> diskManager;
    private final HashTable<Integer, Page<T>> pageTable; // to keep track of pages
    private final Replacer<Page<T>> replacer;   // to find an unpinned page for replacement
    private final ArrayList<Page<T>> freeList; // to find a free page for replacement
    private final Lock mutex = new ReentrantLock(true); // to protect shared data structure
    private final ArrayList<Page<T>> pages;

    public BufferPoolManager(int poolSize, DiskManager<Page<T>> diskManager)
    {
        // number of pages in buffer pool
        this.diskManager = diskManager;
        pages = new ArrayList<>(poolSize);
        pageTable = new ExtendibleHash<>(Constants.BUCKET_SIZE);
        replacer = new LRUReplacer<>();
        freeList = new ArrayList<>(poolSize);

        for(int i = 0; i < poolSize; i++)
        {
            freeList.add(new Page<>());
            pages.add(freeList.get(i));
        }
    }

    private Page<T> getVictimPage()
    {
        Page<T> tar = null;

        if (freeList.isEmpty())
        {
            if (replacer.size() == 0) { return null; }

            tar = replacer.victim();
        }
        else
        {
            tar = freeList.get(0);
            pages.set(pages.indexOf(tar), null);
            freeList.remove(0);

            assert(tar.getPageID() == Constants.INVALID_PAGE_ID);
        }
        assert(Objects.requireNonNull(tar).getPinCount() == 0);

        return tar;
    }

    public Page<T> fetchPage(int pageID)
    {
        mutex.lock();

        Page<T> tar = null;

        try
        {
            tar = pageTable.find(pageID, tar);

            if ( tar != null)
            {
                tar.pinCount++;
                replacer.erase(tar);
                return tar;
            }

            tar = getVictimPage();

            if (tar == null) return tar;

            if (tar.isDirty) diskManager.writePage(tar.getPageID(), tar);

            pageTable.remove(tar.getPageID());
            pageTable.insert(pageID, tar);

            tar = diskManager.readPage(pageID, tar);

            tar.pinCount = 1;
            tar.isDirty = false;
            tar.setPageID((pageID));
            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while fetching the page");
            e.printStackTrace();
            mutex.unlock();
            return null;
        }
        return tar;
    }

    public boolean unpinPage(int pageID, boolean isDirty)
    {
        mutex.lock();

        Page<T> tar = null;

        try
        {
            tar = pageTable.find(pageID, tar);

            if(tar == null) return false;

            tar.isDirty = isDirty;

            if(tar.getPinCount() <= 0) return false;

            if(--tar.pinCount == 0) replacer.insert(tar);

            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while unpinning the page");
            e.printStackTrace();
            mutex.unlock();
            return false;
        }
        return true;
    }

    public boolean flushPage(int pageID)
    {
        mutex.lock();
        Page<T> tar = null;
        try
        {
            pageTable.find(pageID, tar);

            if (tar.getPageID() == Constants.INVALID_PAGE_ID) { return false; }

            if (tar.isDirty)
            {
                diskManager.writePage(pageID, tar);
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

    public Page<T> newPage(int rootPageID)
    {
        mutex.lock();
        Page<T> tar = null;
        try
        {
            tar = getVictimPage();

            if (tar == null) return tar;

            rootPageID = diskManager.allocatePage();
            
            if (tar.isDirty) diskManager.writePage(tar.getPageID(), tar);

            pageTable.remove(tar.getPageID());
            pageTable.insert(rootPageID, tar);

            tar.setPageID(rootPageID);
            tar.isDirty = false;
            tar.pinCount = 1;

            mutex.unlock();
        }
        catch (Exception e)
        {
            System.out.println("Exception occured while creating a new page");
            e.printStackTrace();
            mutex.unlock();
            return null;
        }
        return tar;
    }

    public boolean DeletePage(int pageID)
    {
        mutex.lock(); Page<T> tar = null;
        try
        {
            pageTable.find(pageID, tar);

            if (tar.getPinCount() > 0) return false;

            replacer.erase(tar);
            pageTable.remove(pageID);

            tar.isDirty= false;
            tar.resetMemory();

            freeList.add(tar);

            for(int i = 0; i < pages.size(); i++) {
                if(pages.get(i) == null) {
                    pages.set(i, tar);
                    break;
                }
            }

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
