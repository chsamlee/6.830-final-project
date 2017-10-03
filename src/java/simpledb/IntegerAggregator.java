package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_FIELD = null;

    private final int gbField;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;
    private final Map<Field, AggregateOpAccumulator> aMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.aMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupGbField = (gbField == Aggregator.NO_GROUPING ?
                            NO_GROUPING_FIELD :
                            tup.getField(gbField));
        if (!aMap.containsKey(tupGbField)) {
            AggregateOpAccumulator accumulator;
            if (op == Op.MAX) {
                accumulator = new AggregateOpAccumulator.AggregateMaxAccumulator();
            } else if (op == Op.MIN) {
                accumulator = new AggregateOpAccumulator.AggregateMinAccumulator();
            } else if (op == Op.COUNT) {
                accumulator = new AggregateOpAccumulator.AggregateCountAccumulator();
            } else if (op == Op.SUM) {
                accumulator = new AggregateOpAccumulator.AggregateSumAccumulator();
            } else if (op == Op.AVG) {
                accumulator = new AggregateOpAccumulator.AggregateAvgAccumulator();
            } else {
                throw new UnsupportedOperationException("Unknown op");
            }
            aMap.put(tupGbField, accumulator);
        }
        aMap.get(tupGbField).add(tup.getField(aField));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        Map<Field, Field> aggregates = aMap.entrySet()
                                           .stream()
                                           .collect(Collectors.toMap(
                                                   Map.Entry::getKey,
                                                   e -> e.getValue().aggregate()
                                           ));
        return new AggregatorIterator(aggregates, gbFieldType, Type.INT_TYPE);
    }

}
