package simpledb;

/**
 * Accumulators for aggregation ops.
 */
public abstract class AggregateOpAccumulator {

    public static class AggregateCountAccumulator extends AggregateOpAccumulator {
        private int count = 0;

        @Override
        public void add(Field field) throws ClassCastException {
            count++;
        }

        @Override
        public Field aggregate() {
            return new IntField(count);
        }
    }

    public static class AggregateMaxAccumulator extends AggregateOpAccumulator {
        private Integer maximum = null;

        @Override
        public void add(Field field) throws ClassCastException {
            int value = IntField.class.cast(field).getValue();
            if (maximum == null) {
                maximum = value;
            } else {
                maximum = Math.max(maximum, value);
            }
        }

        @Override
        public Field aggregate() {
            return new IntField(maximum);
        }
    }

    public static class AggregateMinAccumulator extends AggregateOpAccumulator {
        private Integer minimum = null;

        @Override
        public void add(Field field) throws ClassCastException {
            int value = IntField.class.cast(field).getValue();
            if (minimum == null) {
                minimum = value;
            } else {
                minimum = Math.min(minimum, value);
            }
        }

        @Override
        public Field aggregate() {
            return new IntField(minimum);
        }
    }

    public static class AggregateSumAccumulator extends AggregateOpAccumulator {
        private int sum = 0;

        @Override
        public void add(Field field) throws ClassCastException {
            int value = IntField.class.cast(field).getValue();
            sum += value;
        }

        @Override
        public Field aggregate() {
            return new IntField(sum);
        }
    }

    public static class AggregateAvgAccumulator extends AggregateOpAccumulator {
        private int count = 0;
        private int sum = 0;

        @Override
        public void add(Field field) throws ClassCastException {
            int value = IntField.class.cast(field).getValue();
            sum += value;
            count++;
        }

        @Override
        public Field aggregate() {
            int avg = (count > 0 ? sum / count : 0);
            return new IntField(avg);
        }
    }

    /**
     * Add a data point for aggregation.
     * @param field data to insert
     * @throws ClassCastException if obj is not of the correct class
     */
    public abstract void add(Field field) throws ClassCastException;

    /**
     * Compute the aggregated result.
     */
    public abstract Field aggregate();

}
