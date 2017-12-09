package simpledb.json;

import com.google.gson.JsonElement;
import simpledb.Database;
import simpledb.DbException;
import simpledb.IntField;
import simpledb.OpIterator;
import simpledb.Operator;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.io.IOException;

/**
 * Idea:
 * Take in an array of JSON objects, break down each object and feed to Insert operators.
 * JsonScan determines which keys to insert to each table, as well as corrections.
 * JsonScan takes in a JSON object/array as well as initial table ID.
 */
public class JsonInsert extends Operator {

    private TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    private boolean finished = false;
    private JsonScan scanner;

    private TransactionId tid;
    private int tableId;

    public JsonInsert(TransactionId t, JsonElement je, int table) {
        this.scanner = new JsonScan(t, je, table);
        this.tid = t;
        this.tableId = table;
    }

    // return the number of rows inserted in the original table
    // possibly include # of rows inserted in other tables
    @Override
    protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (!finished) {
            int insertCount = 0;
            while (scanner.hasNext()) {
                Tuple tuple = scanner.next();
                int tupleLength = tuple.getTupleDesc().numFields();
                int table = ((IntField) tuple.getField(tupleLength - 1)).getValue();
                Tuple dataTup = Tuple.truncate(tuple, tupleLength - 1);
                try {
                    Database.getBufferPool().insertTuple(tid, table, dataTup);
                } catch (IOException e) {
                    throw new DbException("I/O error when inserting tuple");
                }
                if (table == tableId) {
                    insertCount++;
                }
            }
            finished = true;
            Tuple summaryTuple = new Tuple(tupleDesc);
            summaryTuple.setField(0, new IntField(insertCount));
            return summaryTuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{scanner};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children[0] instanceof JsonScan) {
            scanner = (JsonScan) children[0];
        }
        else {
            throw new IllegalArgumentException("Child is not a JsonScan instance");
        }
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // we don't need to rewind childIter since we don't want to re-insert
    }

    @Override
    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

}
