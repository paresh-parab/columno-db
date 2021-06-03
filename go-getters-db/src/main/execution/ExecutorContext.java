package main.execution;

import main.buffer.BufferPoolManager;
import main.catalog.Catalog;

public class ExecutorContext {
    private Catalog catalog;
    private BufferPoolManager bpm;


    public ExecutorContext(Catalog catalog, BufferPoolManager bpm) {
        this.catalog = catalog;
        this.bpm = bpm;
    }


    public Catalog getCatalog() {
        return catalog;
    }


    public BufferPoolManager getBufferPoolManager() {
        return bpm;
    }


}