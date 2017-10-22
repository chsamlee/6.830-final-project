package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private final int[] buckets;
    private final int min;
    private final int max;
    private final int range;
    private final int firstBucketEnd;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // the maximum number of buckets we ever need is one per number in [min, max]
        if (buckets >= max - min + 1) {
            buckets = max - min + 1;
        }
        // first bucket stores numbers up to but excluding firstBucketEnd
        // all other buckets store `range` numbers each
    	this.buckets = new int[buckets];
    	this.min = min;
    	this.max = max;
    	this.range = (max - min + 1) / buckets;
    	this.firstBucketEnd = max + 1 - (buckets - 1) * range;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = bucketIndex(v);
        buckets[index] = buckets[index] + 1;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    // This version is needed to satisfy the Histogram interface requirement;
    // The primitive version is needed to make tests pass.
    public void addValue(Integer v) {
        addValue((int) v);
    }

    /**
     * Compute the index of the bucket that v should be in.
     */
    private int bucketIndex(int v) {
        if (v < firstBucketEnd) {
            return 0;
        }
        return 1 + (v - firstBucketEnd) / range;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    // This version is needed to satisfy the Histogram interface requirement;
    // The primitive version is needed to make tests pass.
    public double estimateSelectivity(Predicate.Op op, Integer v) {
        return estimateSelectivity(op, (int) v);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        if (v < min) {
            return estimateSelectivityOutOfBounds(op, true);
        }
        else if (v > max) {
            return estimateSelectivityOutOfBounds(op, false);
        }

        int index = bucketIndex(v);
    	int smallBucketsSum = 0;
    	int bigBucketsSum = 0;
    	for (int i = 0; i < index; i++) {
    	    smallBucketsSum += buckets[i];
        }
        for (int i = index + 1; i < buckets.length; i++) {
    	    bigBucketsSum += buckets[i];
        }
        int ntup = smallBucketsSum + bigBucketsSum + buckets[index];

    	int bucketStart = (index == 0) ? min : firstBucketEnd + (index - 1) * range;
    	int bucketEnd = firstBucketEnd + index * range;
    	int bucketSpanSize = bucketEnd - bucketStart;

    	double inBucketRowEst;
    	switch (op) {
            case EQUALS:
                return (double) buckets[index] / bucketSpanSize / ntup;
            case NOT_EQUALS:
                return 1 - (double) buckets[index] / bucketSpanSize / ntup;
            case GREATER_THAN:
                inBucketRowEst = (double) buckets[index] * (bucketEnd - (v + 1)) / bucketSpanSize;
                return (inBucketRowEst + bigBucketsSum) / ntup;
            case GREATER_THAN_OR_EQ:
                inBucketRowEst = (double) buckets[index] * (bucketEnd - v) / bucketSpanSize;
                return (inBucketRowEst + bigBucketsSum) / ntup;
            case LESS_THAN:
                inBucketRowEst = (double) buckets[index] * (v - bucketStart) / bucketSpanSize;
                return (inBucketRowEst + smallBucketsSum) / ntup;
            case LESS_THAN_OR_EQ:
                inBucketRowEst = (double) buckets[index] * (v + 1 - bucketStart) / bucketSpanSize;
                return (inBucketRowEst + smallBucketsSum) / ntup;
            case LIKE:
                // we don't really know anything about the selectivity in this case
                return avgSelectivity();
            default:
                throw new IllegalArgumentException("Unknown op");
        }
    }

    /**
     * Estimate the selectivity of a particular predicate, given that the operand is outside [min, max].
     *
     * @param op Operator
     * @param belowMin true if operand < min, false if operand > max
     * @return Predicted selectivity of this particular operator and value
     */
    private double estimateSelectivityOutOfBounds(Predicate.Op op, boolean belowMin) {
        switch (op) {
            case EQUALS:
            case LIKE:
                return 0;
            case NOT_EQUALS:
                return 1;
            case LESS_THAN:
            case LESS_THAN_OR_EQ:
                return belowMin ? 0 : 1;
            case GREATER_THAN:
            case GREATER_THAN_OR_EQ:
                return belowMin ? 1 : 0;
            default:
                throw new IllegalArgumentException("Unknown op");
        }
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format(
                "IntHistogram(size=%s, min=%s, max=%s)",
                buckets.length,
                min,
                max
        );
    }
}
