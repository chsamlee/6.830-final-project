package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * LockManager coordinates the locks on pages.
 */
public class LockManager {

    private final Map<TransactionId, Map<PageId, Permissions>> holdings;
    private final Map<PageId, UpgradeableLock> pageLocks;

    public LockManager() {
        this.holdings = new HashMap<>();
        this.pageLocks = new HashMap<>();
    }

    private void assertTransactionInManager(TransactionId tid, boolean failSilently) {
        if (!holdings.containsKey(tid)) {
            if (failSilently) {
                holdings.put(tid, new HashMap<>());
            }
            else {
                throw new IllegalArgumentException("Transaction not found");
            }
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
    }

    /**
     * Remove a transaction from the manager after it is finished.
     */
    public void completeTransaction(TransactionId tid) {
        assertTransactionInManager(tid, false);
        if (!holdings.get(tid).isEmpty()) {
            throw new IllegalStateException("This transaction still holds some locks");
        }
        holdings.remove(tid);
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
    public void acquire(TransactionId tid, PageId pid, Permissions perm) {
        assertTransactionInManager(tid, true);
        assertPageInManager(pid);
        if (perm == Permissions.READ_ONLY) {
            pageLocks.get(pid).readLock(tid);
        }
        else if (perm == Permissions.READ_WRITE) {
            pageLocks.get(pid).writeLock(tid);
        }
        else {
            throw new IllegalArgumentException("Unknown permission level");
        }
        if (!holdings.get(tid).containsKey(pid) || perm.compareTo(holdings.get(tid).get(pid)) > 0) {
            holdings.get(tid).put(pid, perm);
        }
    }

    /**
     * Release the lock on a page that is held by some transaction.
     */
    public void release(TransactionId tid, PageId pid) {
        assertTransactionInManager(tid, false);
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
        assertTransactionInManager(tid, false);
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
