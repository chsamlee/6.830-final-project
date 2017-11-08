package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A reentrant readers-writer lock with option to upgrade from read to write.
 * Only one unlock operation is needed, regardless of the number of lock
 * operations called. There is no explicit upgrade operation; writeLock
 * is used for both granting an exclusive lock and upgrading a shared lock.
 *
 * If a thread is interrupted, it will give up on trying to acquiring a lock,
 * but locks that are already acquired need to be manually released.
 */
public class UpgradeableLock {

    private Set<TransactionId> readers;
    private TransactionId writer;

    /**
     * Create an upgradeable lock.
     */
    public UpgradeableLock() {
        readers = new HashSet<>();
        writer = null;
    }

    /**
     * Acquire a shared lock.
     */
    public synchronized void readLock(TransactionId tid) {
        if (readers.contains(tid) || holdsWriteLock(tid)) {
            return;
        }
        while (writer != null) {
            try {
                wait();
            }
            catch (InterruptedException e) {
                return;
            }
        }
        readers.add(tid);
    }

    /**
     * Release a shared lock. If the lock has been upgraded, use writeUnlock instead.
     */
    public synchronized void readUnlock(TransactionId tid) {
        if (!readers.contains(tid)) {
            throw new IllegalMonitorStateException("This transaction doesn't hold a read lock");
        }
        readers.remove(tid);
        notify();
    }

    /**
     * Checks if the exclusive lock is held by the a transaction.
     */
    private synchronized boolean holdsWriteLock(TransactionId tid) {
        return (writer != null && writer.equals(tid));
    }

    /**
     * Acquire the exclusive lock.
     */
    public synchronized void writeLock(TransactionId tid) {
        if (holdsWriteLock(tid)) {
            return;
        }
        while (!readers.isEmpty()) {
            // upgrading a read lock to write lock
            if (readers.size() == 1 && readers.contains(tid)) {
                readers.remove(tid);
                break;
            }
            try {
                wait();
            }
            catch (InterruptedException e) {
                return;
            }
        }
        writer = tid;
    }

    /**
     * Release the exclusive lock.
     */
    public synchronized void writeUnlock(TransactionId tid) {
        if (!holdsWriteLock(tid)) {
            throw new IllegalMonitorStateException("This transaction doesn't hold the write lock");
        }
        writer = null;
        notifyAll();
    }

    /**
     * Checks if the lock (includes both shared and exclusive lock) is held by some transaction.
     */
    public synchronized boolean isHeld() {
        return (!readers.isEmpty() || writer != null);
    }

    public synchronized boolean isHeldByWriter() {
        return (writer != null);
    }

    /**
     * Force all transactions holding this lock to release their locks.
     * THIS IS AN UNSAFE OPERATION
     *
     * @param action operation to perform on each transaction
     *               - the first argument is the transaction ID
     *               - the second argument is true if the transaction holds a shared lock,
     *                 false if the transaction holds the exclusive lock
     */
    public synchronized void forceRelease(BiConsumer<TransactionId, Boolean> action) {
        readers.forEach(tid -> action.accept(tid, false));
        readers.clear();
        if (writer != null) {
            action.accept(writer, true);
            writer = null;
        }
    }

}
