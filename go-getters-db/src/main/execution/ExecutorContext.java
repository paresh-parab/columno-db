package main.execution;

public class ExecutorContext {
    private Transaction transaction;
    private SimpleCatalog catalog;
    private BufferPoolManager bpm;
    /**
     * Creates an ExecutorContext for the transaction that is executing the query.
     * @param transaction the transaction executing the query
     * @param catalog the catalog that the executor should use
     * @param bpm the buffer pool manager that the executor should use
     */
    public ExecutorContext(Transaction transaction, SimpleCatalog catalog, BufferPoolManager bpm) {
        this.transaction = transaction;
        this.catalog = catalog;
        this.bpm = bpm;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    /** @return the catalog */
    public SimpleCatalog getCatalog() {
        return catalog;
    }

    /** @return the buffer pool manager */
    public BufferPoolManager getBufferPoolManager() {
        return bpm;
    }

    /** @return the log manager - don't worry about it for now */
    public LogManager getLogManager() {
        return null;
    }


    /** @return the lock manager - don't worry about it for now */
    public LockManager getLockManager() {
        return null;
    }


}