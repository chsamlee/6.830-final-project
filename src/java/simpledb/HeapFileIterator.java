package simpledb;
import java.util.*;

/**
 * HeapFileIterator is the iterator for HeapFile that traverses through all tuples in the file.
 */
public class HeapFileIterator extends AbstractDbFileIterator {

    private final TransactionId transactionId;
    private final HeapFile file;
    private int pageNumber;
    private Iterator<Tuple> currIterator;

    public HeapFileIterator(HeapFile f, TransactionId tid) {
        this.transactionId = tid;
        this.file = f;
        this.pageNumber = -1;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        while (true) {
            if (currIterator != null && currIterator.hasNext()) {
                // fetch from current page
                return currIterator.next();
            } else if (pageNumber + 1 < file.numPages()) {
                // move to next page and fetch the first tuple in next loop
                pageNumber++;
                BufferPool bufPool = Database.getBufferPool();
                HeapPageId hpid = new HeapPageId(file.getId(), pageNumber);
                HeapPage page = (HeapPage) bufPool.getPage(transactionId, hpid, Permissions.READ_ONLY);
                currIterator = page.iterator();
            } else {
                // run out of pages
                return null;
            }
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        pageNumber = -1;
        currIterator = null;
    }
}
