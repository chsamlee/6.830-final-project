package simpledb;

import java.util.Iterator;
import java.util.Map;

/**
 * An iterator for results of an aggregate function.
 */
public class AggregatorIterator extends Operator {

    private final Map<Field, Field> aggregates;
    private final Type gbType;
    private final Type aType;
    private Iterator<Map.Entry<Field, Field>> iter;

    /**
     * Constructs an iterator.
     * @param aMap mapping from group-by fields to aggregated fields
     * @param gbtype type of group-by fields
     * @param atype type of aggregated fields
     */
    public AggregatorIterator(Map<Field, Field> aMap, Type gbtype, Type atype) {
        this.aggregates = aMap;
        this.gbType = gbtype;
        this.aType = atype;
        this.iter = aggregates.entrySet().iterator();
    }

    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (!iter.hasNext()) {
            return null;
        }
        Map.Entry<Field, Field> entry = iter.next();
        Tuple tuple = new Tuple(getTupleDesc());
        if (gbType == null) {
            tuple.setField(0, entry.getValue());
            return tuple;
        }
        else {
            tuple.setField(0, entry.getKey());
            tuple.setField(1, entry.getValue());
            return tuple;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // no children
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        iter = aggregates.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        if (gbType == null) {
            return new TupleDesc(new Type[]{aType});
        }
        else {
            return new TupleDesc(new Type[]{gbType, aType});
        }
    }

}
