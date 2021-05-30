package main.execution.queryExecutor;

import main.execution.queryCatalog.Catalog;
import main.execution.queryCatalog.RID;
import main.execution.queryCatalog.Transaction;

public class ExContext {
    Catalog catalog;
    Transaction transaction;
    RID rid;

    public ExContext() {
    }

    public ExContext(Catalog catalog, Transaction transaction, RID rid) {
        this.catalog = catalog;
        this.transaction = transaction;
        this.rid = rid;
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    public RID getRid() {
        return rid;
    }

    public void setRid(RID rid) {
        this.rid = rid;
    }
}
