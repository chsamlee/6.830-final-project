package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUPING_FIELD = null;

    private final int gbField;
    private final Type gbFieldType;
    private final Map<Field, AggregateOpAccumulator> aMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only COUNT operator is supported");
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupGbField = (gbField == Aggregator.NO_GROUPING ?
                            NO_GROUPING_FIELD :
                            tup.getField(gbField));
        aMap.putIfAbsent(tupGbField, new AggregateOpAccumulator.AggregateCountAccumulator());
        aMap.get(tupGbField).add(null);  // we can get away with null since the arg isn't used
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
