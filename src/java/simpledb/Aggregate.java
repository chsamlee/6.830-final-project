package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator childIter;
    private int gbField;
    private int aField;
    private Aggregator.Op op;

    private TupleDesc td;
    private Aggregator aggregator;
    private OpIterator aggregatorIter;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    this.childIter = child;
	    this.gbField = gfield;
	    this.aField = afield;
	    this.op = aop;

	    Type aFieldType = child.getTupleDesc().getFieldType(afield);
	    Type gbFieldType = (gbField == -1 ? null : child.getTupleDesc().getFieldType(gfield));
	    if (aFieldType == Type.INT_TYPE) {
	        this.aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        }
        else if (aFieldType == Type.STRING_TYPE) {
	        this.aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
        }
        else {
	        throw new UnsupportedOperationException("Unknown type");
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    return (gbField == -1 ? null : childIter.getTupleDesc().getFieldName(gbField));
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        String aFieldNameRaw = childIter.getTupleDesc().getFieldName(aField);
        return String.format(
                "%s(%s)",
                op.name(),
                aFieldNameRaw == null ? "null" : aFieldNameRaw
        );
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();
	    childIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggregatorIter == null) {
            while (childIter.hasNext()) {
                aggregator.mergeTupleIntoGroup(childIter.next());
            }
            aggregatorIter = aggregator.iterator();
            aggregatorIter.open();
        }
        if (!aggregatorIter.hasNext()) {
            return null;
        }
        Tuple tuple = aggregatorIter.next();
        tuple.resetTupleDesc(getTupleDesc());
        return tuple;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    if (aggregatorIter != null) {
	        aggregatorIter.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if (td == null) {
            TupleDesc outTd = aggregator.iterator().getTupleDesc();
            Type aFieldType = outTd.getFieldType(gbField == -1 ? 0 : 1);
            String aFieldName = aggregateFieldName();
            if (gbField == -1) {
                td = new TupleDesc(new Type[]{aFieldType}, new String[]{aFieldName});
            }
            else {
                Type gbFieldType = outTd.getFieldType(0);
                String gbFieldName = groupFieldName();
                td = new TupleDesc(new Type[]{gbFieldType, aFieldType},
                                   new String[]{gbFieldName, aFieldName});
            }
        }
	    return td;
    }

    public void close() {
        childIter.close();
        if (aggregatorIter != null) {
            aggregatorIter.close();
        }
        super.close();
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
