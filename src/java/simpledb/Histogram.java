package simpledb;

public interface Histogram<T> {

    void addValue(T v);

    double estimateSelectivity(Predicate.Op op, T v);

    double avgSelectivity();

}
