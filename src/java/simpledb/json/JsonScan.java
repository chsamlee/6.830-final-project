package simpledb.json;

import simpledb.DbException;
import simpledb.OpIterator;
import simpledb.TransactionAbortedException;
import simpledb.Tuple;
import simpledb.TupleDesc;

import java.util.NoSuchElementException;

public class JsonScan implements OpIterator {

    @Override
    public void open() throws DbException, TransactionAbortedException {

    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {

    }

    // different tables have different schema, so leave as null
    @Override
    public TupleDesc getTupleDesc() {
        return null;
    }

    @Override
    public void close() {

    }
}
