package main.execution;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;

import java.util.logging.LogManager;

public class ExecutorContext {
    private Catalog catalog;
    private BufferPoolManager bpm;
    /**
     * Creates an ExecutorContext for the transaction that is executing the query.
     * @param transaction the transaction executing the query
     * @param catalog the catalog that the executor should use
     * @param bpm the buffer pool manager that the executor should use
     */
    public ExecutorContext(Catalog catalog, BufferPoolManager bpm) {
        this.catalog = catalog;
        this.bpm = bpm;
    }

    /** @return the catalog */
    public Catalog getCatalog() {
        return catalog;
    }

    /** @return the buffer pool manager */
    public BufferPoolManager getBufferPoolManager() {
        return bpm;
    }


}