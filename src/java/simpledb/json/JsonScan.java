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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Queue;

public class JsonScan implements OpIterator {

    private TransactionId tid;
    private JsonArray ja;
    private int tableId;
    private int index = 0;
    private Queue<Tuple> tupBuffer = new LinkedList<>();
    static private TupleDesc tableToInsertIntoDesc = new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {"_tableid"});

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

        Tuple tup = new Tuple(TupleDesc.merge(td, tableToInsertIntoDesc));
        
        KeyMatching matching = new KeyMatching(obj);
        
        for (int i = 0; i < td.numFields(); i++) {
            String fieldName = td.getFieldName(i);
            Type fieldType = td.getFieldType(i);

            JsonElement matchingElement = matching.getMatchingKey(fieldName, fieldType);

            // Expecting an object
            if (refs.containsKey(i)) {
            	
        		// Got primitive
            	if(matchingElement.isJsonPrimitive()) {
        			int referredTable = refs.get(i);
        			String pKeyName = catalog.getPrimaryKey(referredTable);
        			int pKeyIndex = catalog.getTupleDesc(referredTable).fieldNameToIndex(pKeyName);
        			Type pKeyType = catalog.getTupleDesc(referredTable).getFieldType(pKeyIndex);
        			if(pKeyType == Type.INT_TYPE) {
        				if(!matchingElement.getAsJsonPrimitive().isNumber()) {
                            throw new IllegalArgumentException("Unknown data type");
        				}
        				tup.setField(i, new IntField(matchingElement.getAsJsonPrimitive().getAsInt()));
        			}
        			else if(pKeyType == Type.STRING_TYPE) {
        				tup.setField(i, new StringField(matchingElement.getAsJsonPrimitive().getAsString(), Type.STRING_LEN));
        			}
        		}
            	
            	// Got object
            	else if(matchingElement.isJsonObject()) {
	            		
	                /**
	                 *  TODO: handle array case
	                 *  Suppose we have 2 tables A and B. If B.a is a foreign key to A.a and we
	                 *  want to insert {'a': 1, 'b': [1,2]} into A, ideally we insert 1 row into
	                 *  A, 2 rows into B and link the 2 rows in B to the row in A. However the
	                 *  current implementation ignores 'b' entirely.
	                 */
	                JsonObject val = matchingElement.getAsJsonObject();
	                // recursively process references
	                int refTable = refs.get(i);
	                Tuple refTuple = jsonObjectToTuple(val, refTable);
	                TupleDesc refTd = refTuple.getTupleDesc();
	                // Note: this will fail if a table does not have a pkey column
	                int refColumn = refTd.fieldNameToIndex(catalog.getPrimaryKey(refTable));
	                tup.setField(i, refTuple.getField(refColumn));
	            }
            	
            	// Got something else
            	else {
                    throw new IllegalArgumentException("Unknown data type");
            	}
            }
            // Expecting an integer
            else if(fieldType == Type.INT_TYPE) {
            	if(matchingElement.isJsonPrimitive()) {
	                JsonPrimitive val = matchingElement.getAsJsonPrimitive();
	                
	                // Got number
	                if (val.isNumber()) {
	                    tup.setField(i, new IntField(val.getAsInt()));
	                }
	                
	                // Got string
	                else if (val.isString()) {
	                    tup.setField(i, new IntField(Integer.parseInt(val.getAsString())));
	                }
            	}
            	
            	// Got anything else
                else {
                    throw new IllegalArgumentException("Unknown data type");
                }
            }
            
            // Expecting a string
            else if(fieldType == Type.STRING_TYPE){
            	if(matchingElement.isJsonPrimitive()) {
	                JsonPrimitive val = matchingElement.getAsJsonPrimitive();
	                
	                // Got a string
	                if (val.isString()) {
	                    tup.setField(i, new StringField(val.getAsString(), Type.STRING_LEN));
	                }
	                
	                // Got another primitive
	                else {
	                    tup.setField(i, new StringField(val.toString(), Type.STRING_LEN));
	                }
            	}
            	
            	// Got anything else
                else {
                    throw new IllegalArgumentException("Unknown data type");
                }
            }
        }
        
        // Expected nothing
        for(Entry<String, JsonElement> unmatchedEntry : matching.unusedKeyValues()) {
        	JsonElement element = unmatchedEntry.getValue();
        	// Got any primitive
        	if(element.isJsonPrimitive()) {
        		logExtraField(element);
        	}
        	
        	// Got an object
        	if(element.isJsonObject()) {
        		for(Iterator<Integer> tableIdIter = catalog.tableIdIterator(); tableIdIter.hasNext();) {
        			int nextTable = tableIdIter.next();
        			if(catalog.getTableName(nextTable) == "") {
        				
        			}
        			Map<Integer, Integer> foreignKeys = catalog.getForeignKeys(nextTable);
        			for(Entry<Integer, Integer> foreignKey : foreignKeys.entrySet()) {
        				if(foreignKey.getValue()==table) {
        					
        				}
        			}
        			//foreignKeys.
        		}
        	}
        }

        // append table info to the tuple
        TupleDesc infoTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
        Tuple infoTup = new Tuple(infoTd);
        infoTup.setField(0, new IntField(table));
        Tuple mergedTup = Tuple.merge(tup, infoTup);
        // append to buffer - we do this as a side effect to make recusion easier
        tupBuffer.add(mergedTup);
        return mergedTup;
    }

	private void logExtraField(JsonElement element) {
		// TODO Auto-generated method stub
	}

}
