package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private final int[] buckets;
    private final int min;
    private final int max;
    private final int range;
    private final int firstBucketEnd;
    private int ntup;

    // variables related to counting distinct elements; we use HyperLogLog (HLL)
    // https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
    private static final int HASH_BUCKET_BITS = 8;
    private static final int HASH_BUCKET_COUNT = 1 << HASH_BUCKET_BITS;
    private static final double HLL_CORRECTION_CONST = 0.7213 / (1 + 1.079 / HASH_BUCKET_COUNT);
    private final int[] hllBuckets;

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
        this.ntup = 0;
        this.hllBuckets = new int[HASH_BUCKET_COUNT];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int index = bucketIndex(v);
        buckets[index] = buckets[index] + 1;
        ntup++;
        // HLL update
        int hv = hashInt(v);
        int hashBucketIndex = hv >>> (32 - HASH_BUCKET_BITS);
        int remainder = (hv << HASH_BUCKET_BITS) >>> HASH_BUCKET_BITS;
        hllBuckets[hashBucketIndex] = Math.max(
                hllBuckets[hashBucketIndex],
                32 - HASH_BUCKET_BITS - highestOneBitLocation(remainder)
        );
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

    private int bucketStart(int index) {
        return (index == 0) ? min : firstBucketEnd + (index - 1) * range;
    }

    private int bucketEnd(int index) {
        return firstBucketEnd + index * range;
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
        for (int i = 0; i < index; i++) {
            smallBucketsSum += buckets[i];
        }
        int bigBucketsSum = ntup - smallBucketsSum - buckets[index];

        int bucketStart = bucketStart(index);
        int bucketEnd = bucketEnd(index);
        int bucketSpanSize = bucketEnd - bucketStart;

        double inBucketRowEst;
        switch (op) {
            case EQUALS:
            case LIKE:
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

    /**
     * Estimate the selectivity of a join between two IntHistograms.
     */
    public static double estimateJoinSelectivity(IntHistogram h1, IntHistogram h2, Predicate.Op op) {
        if (op == Predicate.Op.EQUALS || op == Predicate.Op.LIKE) {
            // assume that each value in the smaller table has a matching value in the larger table
            // for equality case, estimation by sampling is only good if the datasets are unrelated
            return 1.0 / Math.max(h1.estimateDistinctElements(), h2.estimateDistinctElements());
        }
        else if (op == Predicate.Op.NOT_EQUALS) {
            return 1 - estimateJoinSelectivity(h1, h2, Predicate.Op.EQUALS);
        }
        else {
            // estimate by sampling and take the weighted average of estimated selectivities
            int SAMPLES_PER_BUCKET = 2;
            double selectivity = 0;
            for (int i = 0; i < h2.buckets.length; i++) {
                int bucketStart = h2.bucketStart(i);
                int bucketEnd = h2.bucketEnd(i);
                for (int j = 0; j < SAMPLES_PER_BUCKET; j++) {
                    int sample = (int) (bucketStart + Math.random() * (bucketEnd - bucketStart));
                    double sampleSelectivity = h1.estimateSelectivity(op, sample);
                    selectivity += sampleSelectivity * h2.buckets[i] / h2.ntup / SAMPLES_PER_BUCKET;
                }
            }
            return selectivity;
        }
    }

    /**
     * @return An estimate of the number of distinct elements in this histogram.
     */
    private int estimateDistinctElements() {
        // in the case where we have few data points in the histogram, just return the
        // sum of expected number of distinct elements in each bucket
        // https://math.stackexchange.com/a/72351
        if (ntup < HASH_BUCKET_COUNT * 5) {
            double expected = 0;
            for (int i = 0; i < buckets.length; i++) {
                int n = bucketStart(i) - bucketEnd(i);
                expected += n * (1 - Math.pow(1 - 1.0 / n, buckets[i]));
            }
            return (int) expected;
        }
        // use HLL algorithm if we have enough data points
        double sum = Arrays.stream(hllBuckets).mapToDouble(n -> Math.pow(2.0, -n)).sum();
        double rawEstimate = HLL_CORRECTION_CONST * Math.pow(HASH_BUCKET_COUNT, 2) / sum;
        if (rawEstimate <= HASH_BUCKET_COUNT / 2 * 5) {
            int zeroHashBuckets = (int) Arrays.stream(hllBuckets).filter(n -> n != 0).count();
            if (zeroHashBuckets != 0) {
                return (int) (HASH_BUCKET_COUNT * Math.log(HASH_BUCKET_COUNT / rawEstimate));
            }
            else {
                return (int) rawEstimate;
            }
        }
        else if (rawEstimate <= (1L << 32) / 30) {
            return (int) rawEstimate;
        }
        else {
            return (int) (-Math.log(1 - rawEstimate / (1L << 32)) * (1L << 32));
        }
    }

    // hash function to randomize the input values; used for HLL
    // https://stackoverflow.com/a/12996028
    private static int hashInt(int n) {
        n = ((n >>> 16) ^ n) * 0x45d9f3b;
        n = ((n >>> 16) ^ n) * 0x45d9f3b;
        n = (n >>> 16) ^ n;
        return n;
    }

    // compute the location of highest one bit; -1 if n = 0; used for HLL
    // e.g. 1 = 0b1 -> 0, 13 = 0b1101 -> 3
    private static int highestOneBitLocation(int n) {
        if (n == 0) {
            return -1;
        }
        int hob = Integer.highestOneBit(n);
        int location = 0;
        while (hob != (1 << location)) {
            location++;
        }
        return location;
    }

}
