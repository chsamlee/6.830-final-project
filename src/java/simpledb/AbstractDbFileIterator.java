package simpledb;

import java.util.NoSuchElementException;

/** Helper for implementing DbFileIterators. Handles hasNext()/next() logic. */
public abstract class AbstractDbFileIterator implements DbFileIterator {

    /*
     * The default implementation fails if we close the iterator when there
     * are still elements in the iterator, then call next(). Modified the file
     * to handle open()/close() logic properly.
     */

    private boolean isOpen = false;
    private Tuple next = null;

    @Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
	    if (!isOpen) {
	        return false;
        }
        if (next == null) {
            next = readNext();
        }
        return next != null;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
	    if (!isOpen) {
	        throw new NoSuchElementException();
        }
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    /** If subclasses override this, they should call super.open(). */
    @Override
    public void open() throws DbException, TransactionAbortedException {
	    isOpen = true;
    }

    /** If subclasses override this, they should call super.close(). */
    @Override
    public void close() {
        isOpen = false;
    }

    /** Reads the next tuple from the underlying source.
    @return the next Tuple in the iterator, null if the iteration is finished. */
    protected abstract Tuple readNext() throws DbException, TransactionAbortedException;
}
