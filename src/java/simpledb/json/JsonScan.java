package simpledb.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import simpledb.Catalog;
import simpledb.Database;
import simpledb.DbException;
import simpledb.IntField;
import simpledb.OpIterator;
import simpledb.StringField;
import simpledb.TransactionAbortedException;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;

import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

public class JsonScan implements OpIterator {

    private TransactionId tid;
    private JsonArray ja;
    private int tableId;
    private int index = 0;
    private Queue<Tuple> tupBuffer = new LinkedList<>();

    public JsonScan(TransactionId t, JsonElement je, int table) {
        this.tid = t;
        this.tableId = table;
        if (je.isJsonArray()) {
            this.ja = je.getAsJsonArray();
        }
        else if (je.isJsonObject()) {
            this.ja = new JsonArray();
            this.ja.add(je);
        }
        else {
            throw new IllegalArgumentException("Illegal type");
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // nothing to open
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return !(index == ja.size() && tupBuffer.isEmpty());
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (tupBuffer.isEmpty()) {
            if (index == ja.size()) {
                throw new NoSuchElementException();
            }
            // refill tupBuffer
            JsonObject jo = (JsonObject) ja.get(index);
            jsonObjectToTuple(jo, tableId);
            index++;
        }
        return tupBuffer.poll();
    }


    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        tupBuffer.clear();
        index = 0;
    }

    @Override
    public TupleDesc getTupleDesc() {
        // different tables have different schema
        return null;
    }

    @Override
    public void close() {
        // nothing to close
    }

    /**
     * Coerce a JSON object into a tuple based on a table.
     * The tuple is automatically inserted into buffer.
     */
    private Tuple jsonObjectToTuple(JsonObject obj, int table) {
        Catalog catalog = Database.getCatalog();
        TupleDesc td = catalog.getTupleDesc(table);
        Map<Integer, Integer> refs = catalog.getForeignKeys(table);

        Tuple tup = new Tuple(td);
        for (int i = 0; i < td.numFields(); i++) {
            String fieldName = td.getFieldName(i);
            Type fieldType = td.getFieldType(i);

            JsonElement rawVal = obj.get(fieldName);
            if (rawVal == null) {
                /**
                 * TODO: handle cases where the field has no corresponding key
                 * Update fieldName and/or rawVal as necessary.
                 */
            }

            if (refs.containsKey(i)) {
                /**
                 *  TODO: handle array case
                 *  Suppose we have 2 tables A and B. If B.a is a foreign key to A.a and we
                 *  want to insert {'a': 1, 'b': [1,2]} into A, ideally we insert 1 row into
                 *  A, 2 rows into B and link the 2 rows in B to the row in A. However the
                 *  current implementation ignores 'b' entirely.
                 */
                JsonObject val = rawVal.getAsJsonObject();
                // recursively process references
                int refTable = refs.get(i);
                Tuple refTuple = jsonObjectToTuple(val, refTable);
                TupleDesc refTd = refTuple.getTupleDesc();
                // Note: this will fail if a table does not have a pkey column
                int refColumn = refTd.fieldNameToIndex(catalog.getPrimaryKey(refTable));
                tup.setField(i, refTuple.getField(refColumn));
            }
            else {
                JsonPrimitive val = rawVal.getAsJsonPrimitive();
                if (fieldType == Type.INT_TYPE) {
                    tup.setField(i, new IntField(val.getAsInt()));
                }
                else if (fieldType == Type.STRING_TYPE) {
                    tup.setField(i, new StringField(val.getAsString(), Type.STRING_LEN));
                }
                else {
                    throw new IllegalArgumentException("Unknown data type");
                }
            }
        }

        /**
         * TODO: dump the extra fields into tuples
         * You may find JsonObject.entrySet function helpful.
         */

        // append table info to the tuple
        TupleDesc infoTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
        Tuple infoTup = new Tuple(infoTd);
        infoTup.setField(0, new IntField(table));
        Tuple mergedTup = Tuple.merge(tup, infoTup);
        // append to buffer - we do this as a side effect to make recusion easier
        tupBuffer.add(mergedTup);
        return mergedTup;
    }

}
