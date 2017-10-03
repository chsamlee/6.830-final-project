package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator childIter;
    private TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean finished = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
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
        // we don't need to rewind childIter since we don't want to re-delete
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!finished) {
            int deleteCount = 0;
            while (childIter.hasNext()) {
                Tuple tuple = childIter.next();
                try {
                    Database.getBufferPool().deleteTuple(tid, tuple);
                } catch (IOException e) {
                    throw new DbException("I/O error when deleting tuple");
                }
                deleteCount++;
            }
            finished = true;
            Tuple summaryTuple = new Tuple(tupleDesc);
            summaryTuple.setField(0, new IntField(deleteCount));
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
