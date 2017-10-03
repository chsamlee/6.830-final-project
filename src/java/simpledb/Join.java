package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private static final int OUTER_BATCH_SIZE = 100;

    private JoinPredicate pred;
    private OpIterator childIter1;
    private OpIterator childIter2;

    private ArrayList<Tuple> outerTuples;
    private Iterator<Tuple> outerTuplesIter;
    private Tuple innerTuple;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     *
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        this.pred = p;
        this.childIter1 = child1;
        this.childIter2 = child2;
        this.outerTuples = new ArrayList<>();
    }

    public JoinPredicate getJoinPredicate() {
        return pred;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return childIter1.getTupleDesc().getFieldName(pred.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return childIter2.getTupleDesc().getFieldName(pred.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return TupleDesc.merge(childIter1.getTupleDesc(),
                childIter2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        childIter1.open();
        childIter2.open();
        refillOuterTuples();
    }

    public void close() {
        childIter1.close();
        childIter2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        childIter1.rewind();
        childIter2.rewind();
        refillOuterTuples();
    }

    private void refillOuterTuples() throws TransactionAbortedException, DbException {
        outerTuples.clear();
        for (int i = 0; i < OUTER_BATCH_SIZE; i++) {
            if (!childIter1.hasNext()) {
                break;
            }
            outerTuples.add(childIter1.next());
        }
        outerTuplesIter = outerTuples.iterator();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // block-nested loops that reads 100 outer tuples at a time
        while (true) {
            // degenerate case: childIter1 is empty
            if (outerTuples.isEmpty() && !childIter1.hasNext()) {
                return null;
            }
            // if innerTuple != null, then we aren't finished with outerTuples yet
            if (innerTuple == null) {
                if (childIter2.hasNext()) {
                    innerTuple = childIter2.next();
                } else {
                    // finished with inner loop, move on to next set of outerTuples
                    childIter2.rewind();
                    if (childIter2.hasNext()) {
                        innerTuple = childIter2.next();
                    } else {
                        // degenerate case: childIter2 is empty
                        return null;
                    }
                    refillOuterTuples();
                    if (outerTuples.isEmpty()) {
                        // we have obtained all tuples in childIter1
                        return null;
                    }
                }
            }
            // iterate through outerTuples and filter
            while (outerTuplesIter.hasNext()) {
                Tuple outerTuple = outerTuplesIter.next();
                if (pred.filter(outerTuple, innerTuple)) {
                    return Tuple.merge(outerTuple, innerTuple);
                }
            }
            outerTuplesIter = outerTuples.iterator();
            innerTuple = null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{childIter1, childIter2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        childIter1 = children[0];
        childIter2 = children[1];
    }

}
