package main.execution.queryCatalog;

import java.util.HashSet;
import java.util.Set;

public class Transaction {
    Set<RID> sharedLockSet = new HashSet<RID>();
    Set<RID> exclusiveLockSet = new HashSet<RID>();

    public Transaction(Integer txnId) {

    }
}
