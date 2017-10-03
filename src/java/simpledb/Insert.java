package simpledb;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private OpIterator childIter;
    private TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean finished = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.tableId = tableId;
        this.childIter = child;
    }

    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        childIter.open();
    }

    public void close() {
        childIter.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // we don't need to rewind childIter since we don't want to re-insert
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!finished) {
            int insertCount = 0;
            while (childIter.hasNext()) {
                Tuple tuple = childIter.next();
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, tuple);
                } catch (IOException e) {
                    throw new DbException("I/O error when inserting tuple");
                }
                insertCount++;
            }
            finished = true;
            Tuple summaryTuple = new Tuple(tupleDesc);
            summaryTuple.setField(0, new IntField(insertCount));
            return summaryTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{childIter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        childIter = children[0];
    }

}
