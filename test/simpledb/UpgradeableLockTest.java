package simpledb;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UpgradeableLockTest {

    private static TransactionId[] tids;
    private static UpgradeableLock lock;
    private static int counter;

    private void noInterruptSleep(long millseconds) {
        try {
            Thread.sleep(millseconds);
        } catch (InterruptedException e) {
            throw new IllegalStateException("Thread interrupted");
        }
    }

    @BeforeClass public static void setUpClass() {
        tids = new TransactionId[4];
        for (int i = 0; i < tids.length; i++) {
            tids[i] = new TransactionId();
        }
    }

    @Before public void setUp() {
        lock = new UpgradeableLock();
        counter = 0;
    }

    @Test public void testMultipleReaders() throws InterruptedException {
        Thread th1 = new Thread(() -> {
            lock.readLock(tids[1]);
            lock.readLock(tids[1]);
            noInterruptSleep(500);
            lock.readUnlock(tids[1]);
        });
        Thread th2 = new Thread(() -> {
            lock.readLock(tids[2]);
            noInterruptSleep(500);
            lock.readUnlock(tids[2]);
        });

        long start = System.currentTimeMillis();
        th1.start();
        th2.start();
        th1.join();
        th2.join();
        long end = System.currentTimeMillis();
        assertTrue(end - start < 600);
    }

    @Test public void testWriterExclusion() throws InterruptedException {
        Thread th1 = new Thread(() -> {
            lock.writeLock(tids[1]);
            counter += 1;
            noInterruptSleep(500);
            lock.writeUnlock(tids[1]);
        });
        Thread th2 = new Thread(() -> {
            lock.writeLock(tids[2]);
            counter += 1;
            noInterruptSleep(500);
            lock.writeUnlock(tids[2]);
        });

        long start = System.currentTimeMillis();
        lock.readLock(tids[0]);
        th1.start();
        th2.start();
        noInterruptSleep(500);
        lock.readUnlock(tids[0]);
        th1.join();
        th2.join();
        long end = System.currentTimeMillis();
        assertTrue(counter == 2);
        assertTrue(end - start > 1400);
    }

    @Test public void testMultipleReadersAfterWrite() throws InterruptedException {
        Thread th1 = new Thread(() -> {
            lock.readLock(tids[1]);
            noInterruptSleep(500);
            assertTrue(counter == 1);
            lock.readUnlock(tids[1]);
        });
        Thread th2 = new Thread(() -> {
            lock.readLock(tids[2]);
            noInterruptSleep(500);
            assertTrue(counter == 1);
            lock.readUnlock(tids[2]);
        });

        long start = System.currentTimeMillis();
        lock.writeLock(tids[0]);
        th1.start();
        th2.start();
        counter += 1;
        noInterruptSleep(500);
        lock.writeUnlock(tids[0]);
        th1.join();
        th2.join();
        long end = System.currentTimeMillis();
        assertTrue(end - start < 1100);
    }

    @Test public void testUpgrade() throws InterruptedException {
        Thread th1 = new Thread(() -> {
            lock.readLock(tids[1]);
            assertTrue(counter == 0);
            noInterruptSleep(500);
            lock.readUnlock(tids[1]);
        });
        Thread th2 = new Thread(() -> {
            lock.readLock(tids[2]);
            noInterruptSleep(500);
            assertTrue(counter == 1);
            lock.readUnlock(tids[2]);
        });

        long start = System.currentTimeMillis();
        lock.readLock(tids[0]);
        th1.start();
        noInterruptSleep(100);
        lock.writeLock(tids[0]);
        counter += 1;
        th2.start();
        noInterruptSleep(500);
        lock.writeUnlock(tids[0]);
        th1.join();
        th2.join();
        long end = System.currentTimeMillis();
        assertTrue(end - start > 1400);
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(UpgradeableLockTest.class);
    }

}
