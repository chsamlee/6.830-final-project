package simpledb.json;

import simpledb.DbException;
import simpledb.OpIterator;
import simpledb.Operator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

/**
 * Idea:
 * Take in an array of JSON objects, break down each object and feed to Insert operators.
 * JsonScan determines which keys to insert to each table, as well as corrections.
 * JsonScan takes in a JSON object/array as well as initial table ID.
 */
public class JsonInsert extends Operator {

    // return the number of rows inserted in the original table
    // possibly include # of rows inserted in other tables
    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        return null;
    }

    // 1 child - JSON object array
    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[0];
    }

    @Override
    public void setChildren(OpIterator[] children) {

    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }

    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

}
