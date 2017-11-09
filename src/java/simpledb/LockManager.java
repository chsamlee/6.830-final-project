package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * LockManager coordinates the locks on pages.
 */
public class LockManager {

    private final Map<TransactionId, Map<PageId, Permissions>> holdings;
    private final Map<PageId, UpgradeableLock> pageLocks;
    private final PrecedenceGraph pGraph;

    public LockManager() {
        this.holdings = new HashMap<>();
        this.pageLocks = new HashMap<>();
        this.pGraph = new PrecedenceGraph();
    }

    private void assertTransactionInManager(TransactionId tid) {
        if (!holdings.containsKey(tid)) {
            holdings.put(tid, new HashMap<>());
            // throw new IllegalArgumentException("Transaction not found");
        }
    }

    /**
     * Add a new transaction to the manager.
     */
    public void addTransaction(TransactionId tid) {
        if (holdings.containsKey(tid)) {
            throw new IllegalArgumentException("This transaction is already added");
        }
        holdings.put(tid, new HashMap<>());
        pGraph.addTransaction(tid);
    }

    /**
     * Remove a transaction from the manager after it is finished.
     */
    public void completeTransaction(TransactionId tid) {
        assertTransactionInManager(tid);
        if (!holdings.get(tid).isEmpty()) {
            throw new IllegalStateException("This transaction still holds some locks");
        }
        holdings.remove(tid);
        pGraph.removeTransaction(tid);
    }

    private void assertPageInManager(PageId pid) {
        if (!pageLocks.containsKey(pid)) {
            pageLocks.put(pid, new UpgradeableLock());
            // throw new IllegalArgumentException("Page not found");
        }
    }

    /**
     * Add a page to the manager.
     */
    public void addPage(PageId pid) {
        pageLocks.putIfAbsent(pid, new UpgradeableLock());
    }

    /**
     * Remove a page from the manager after it is removed from cache.
     */
    public void removePage(PageId pid) {
        assertPageInManager(pid);
        if (pageLocks.get(pid).isHeld()) {
            throw new IllegalArgumentException("Page is in use");
        }
        pageLocks.remove(pid);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return (holdings.containsKey(tid) && holdings.get(tid).containsKey(pid));
    }

    public boolean pageIsLocked(PageId pid) {
        return pageLocks.get(pid).isHeld();
    }

    public boolean pageIsLockedByWriter(PageId pid) {
        return pageLocks.get(pid).isHeldByWriter();
    }

    /**
     * Acquire a lock for a certain page on behalf of a transaction.
     */
    public void acquire(TransactionId tid, PageId pid, Permissions perm) throws DeadlockException {
        System.out.println("Transaction " + tid + " trying to acquire " + pid + ", perm=" + perm);
        assertTransactionInManager(tid);
        assertPageInManager(pid);
        synchronized (this) {
            if (perm == Permissions.READ_ONLY) {
                TransactionId writerTid = pageLocks.get(pid).getWriter();
                System.out.println("RO acquire called; writer=" + writerTid);
                if (writerTid != null && !writerTid.equals(tid)) {
                    pGraph.addDependency(writerTid, tid);
                }
                // pageLocks.get(pid).readLock(tid);
            } else if (perm == Permissions.READ_WRITE) {
                TransactionId writerTid = pageLocks.get(pid).getWriter();
                System.out.println("RW acquire called; writer=" + writerTid);
                if (writerTid != null && !writerTid.equals(tid)) {
                    pGraph.addDependency(writerTid, tid);
                }
                for (TransactionId readerTid : pageLocks.get(pid).getReaders()) {
                    if (!readerTid.equals(tid)) {
                        System.out.println("reader includes " + readerTid);
                        pGraph.addDependency(readerTid, tid);
                    }
                }
                // pageLocks.get(pid).writeLock(tid);
            } else {
                throw new IllegalArgumentException("Unknown permission level");
            }
        }
        if (perm == Permissions.READ_ONLY) {
            pageLocks.get(pid).readLock(tid);
        }
        else if (perm == Permissions.READ_WRITE) {
            pageLocks.get(pid).writeLock(tid);
        }
        if (!holdings.get(tid).containsKey(pid) || perm.compareTo(holdings.get(tid).get(pid)) > 0) {
            holdings.get(tid).put(pid, perm);
        }
    }

    /**
     * Release the lock on a page that is held by some transaction.
     */
    public void release(TransactionId tid, PageId pid) {
        assertTransactionInManager(tid);
        assertPageInManager(pid);
        Permissions perm = holdings.get(tid).get(pid);
        if (perm == Permissions.READ_ONLY) {
            pageLocks.get(pid).readUnlock(tid);
        }
        else if (perm == Permissions.READ_WRITE) {
            pageLocks.get(pid).writeUnlock(tid);
        }
        else {
            throw new IllegalArgumentException("Unknown permission level");
        }
        holdings.get(tid).remove(pid);
    }

    /**
     * Release all locks on a page. UNSAFE.
     */
    public void releasePage(PageId pid) {
        assertPageInManager(pid);
        UpgradeableLock lock = pageLocks.get(pid);
        lock.forceRelease((tid, isReader) -> release(tid, pid));
    }

    /**
     * Release all locks on a transaction.
     */
    public void releaseTransaction(TransactionId tid) {
        assertTransactionInManager(tid);
        holdings.get(tid).forEach((pid, perm) -> {
            if (perm == Permissions.READ_ONLY) {
                pageLocks.get(pid).readUnlock(tid);
            }
            else if (perm == Permissions.READ_WRITE) {
                pageLocks.get(pid).writeUnlock(tid);
            }
            else {
                throw new IllegalArgumentException("Unknown permission level");
            }
        });
        holdings.get(tid).clear();
    }

}
