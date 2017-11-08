package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException("Incorrect table");
        }
        if (pid.getPageNumber() >= numPages()) {
            throw new IllegalArgumentException("Invalid page number");
        }
        int PAGE_SIZE = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * PAGE_SIZE;
        byte[] data = new byte[PAGE_SIZE];
        HeapPageId hpid = new HeapPageId(getId(), pid.getPageNumber());
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(offset);
            raf.readFully(data);
            raf.close();
            return new HeapPage(hpid, data);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        HeapPage hPage = (HeapPage) page;
        HeapPageId pid = hPage.getId();
        if (pid.getTableId() != getId()) {
            throw new IllegalArgumentException("Incorrect table");
        }
        if (pid.getPageNumber() > numPages()) {
            throw new IllegalArgumentException("Invalid page number");
        }
        int PAGE_SIZE = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * PAGE_SIZE;
        byte[] data = hPage.getPageData();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.seek(offset);
        raf.write(data);
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // first try to insert the tuple in an existing page
        int pageCount = numPages();
        for (int pageNumber = 0; pageNumber < pageCount; pageNumber++) {
            HeapPageId pid = new HeapPageId(getId(), pageNumber);
            HeapPage page = (HeapPage) Database.getBufferPool()
                                               .getPage(tid, pid, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool()
                                          .getPage(tid, pid, Permissions.READ_WRITE);
                page.insertTuple(t);
                return new ArrayList<>(Collections.singletonList(page));
            }
            else {
                Database.getBufferPool().releasePage(tid, pid);
            }
        }
        // all pages are full, create a new page
        HeapPageId pid = new HeapPageId(getId(), pageCount);
        HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());
        writePage(newPage);
        // access the new page via BufferPool
        HeapPage page = (HeapPage) Database.getBufferPool()
                                           .getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId()) {
            throw new DbException("The tuple is not on this file");
        }
        HeapPage page = (HeapPage) Database.getBufferPool()
                                           .getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singletonList(page));
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

