package simpledb.json;

import com.google.gson.*;
import java.util.*;
import java.util.Map.Entry;

import org.omg.CORBA.PRIVATE_MEMBER;

import simpledb.*;

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
                    		// TODO: It seems like the user thought the primary key was a string. So suggest a schema update to make the first string field the primary key, if one exists.
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
            		// TODO: If this is an array, it seems like the user thought a 1-1 was a 1-many. So suggest a schema update.
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
                	// TODO: If this is an object, it seems the user thought this was a foreign key, but it's not. Suggest this field should be a foreign key.
                	// TODO: If this is an array, it seems the user thinks there's a foreign key that points here. Try inserting these in another table, and use this field as some aggregate, like a count.
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
                	// TODO: If this is an object, it seems the user thought this was a foreign key, but it's not. Suggest this field should be a foreign key.
                	// TODO: If this is an array, it seems the user thinks there's a foreign key that points here. Try inserting these in another table, and use this field as some aggregate.
                    throw new IllegalArgumentException("Unknown data type");
                }
            }
        }
        
        // Expected nothing
        for(Entry<String, JsonElement> unmatchedEntry : matching.unusedKeyValues()) {
        	JsonElement element = unmatchedEntry.getValue();
        	String key = unmatchedEntry.getKey();
        	// Got any primitive
        	if(element.isJsonPrimitive()) {
        		logExtraField(element);
        	}
        	
        	// Got an object
        	if(element.isJsonObject()) {
        		for(Iterator<Integer> tableIdIter = catalog.tableIdIterator(); tableIdIter.hasNext();) {
        			int foreignTable = tableIdIter.next();
        			if(KeyMatching.matchesApproximately(catalog.getTableName(foreignTable), key)) {
	        			Map<Integer, Integer> foreignKeys = catalog.getForeignKeys(foreignTable);
	        			if(foreignKeys.containsValue(table)) {
	        				// If we get here, there exists a table with a name like this attribute, that has a foreign key back to
	        				// this table. So try to insert this object in that table, after ensuring the foreign key points to this object.
	        				// This is a 1-1 relationship.
	        				Integer localPrimaryKeyIndex = td.fieldNameToIndex(catalog.getPrimaryKey(table));
	        				Field primaryKeyField = tup.getField(localPrimaryKeyIndex);
	    	                JsonObject val = element.getAsJsonObject();
	    	                String foreignKey = catalog.getTupleDesc(foreignTable).getFieldName((getKeyByValue(foreignKeys, table)));
	    	                val.add(foreignKey, fieldToPrimitive(primaryKeyField));
	    	                // recursively process references
	    	                jsonObjectToTuple(val, foreignTable);
		        			break;
	        			}
        			}
        		}
        	}
        	
        	// Got an array
        	else if(element.isJsonArray()) {
        		for(Iterator<Integer> tableIdIter = catalog.tableIdIterator(); tableIdIter.hasNext();) {
        			int foreignTable = tableIdIter.next();
        			if(KeyMatching.matchesApproximately(catalog.getTableName(foreignTable), key) || KeyMatching.matchesApproximately(catalog.getTableName(foreignTable), key+"s")) {
	        			Map<Integer, Integer> foreignKeys = catalog.getForeignKeys(foreignTable);
	        			if(foreignKeys.containsValue(table)) {
	        				// If we get here, there exists a table with a name like this attribute, that has a foreign key back to
	        				// this table. So try to insert all objects in the array into that table, after ensuring the foreign keys point to this object.
	        				// This is a 1-many relationship.
			        		for(JsonElement arrayElement : element.getAsJsonArray()) {
		        				Integer localPrimaryKeyIndex = td.fieldNameToIndex(catalog.getPrimaryKey(table));
		        				Field primaryKeyField = tup.getField(localPrimaryKeyIndex);
		        				if(arrayElement.isJsonObject()) {
		        					JsonObject val = arrayElement.getAsJsonObject();
		        					String foreignKey = catalog.getTupleDesc(foreignTable).getFieldName((getKeyByValue(foreignKeys, table)));
		        					val.add(foreignKey, fieldToPrimitive(primaryKeyField));
		        				}
			        		}
							jsonArrayToTuple(element.getAsJsonArray(), foreignTable);
	        			}
        			}
        		}
        	}
        }

        // append table info to the tuple
        TupleDesc infoTd = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
        Tuple infoTup = new Tuple(infoTd);
        infoTup.setField(0, new IntField(table));
        Tuple mergedTup = Tuple.merge(tup, infoTup);
        // append to buffer - we do this as a side effect to make recursion easier
        tupBuffer.add(mergedTup);
        return mergedTup;
    }
    
    private List<Tuple> jsonArrayToTuple(JsonArray array, int table) {
        Catalog catalog = Database.getCatalog();
        TupleDesc td = catalog.getTupleDesc(table);

    	List<Tuple> tuples = new ArrayList<Tuple>();
		for(JsonElement arrayElement : array) {
			if(arrayElement.isJsonObject()) {
				JsonObject object = arrayElement.getAsJsonObject();
				tuples.add(jsonObjectToTuple(object, table));
			}
		}
		return tuples;
    }

	private void logExtraField(JsonElement element) {
		// TODO Auto-generated method stub
	}

	public static <T,E> T getKeyByValue(Map<T, E> map, E value) {
	    for (Entry<T, E> entry : map.entrySet()) {
	        if (Objects.equals(value, entry.getValue())) {
	        	return entry.getKey();
	        }
	    }
	    return null;
	}
	
	private static JsonPrimitive fieldToPrimitive(Field field) {
		switch(field.getType()) {
		case INT_TYPE:
			return new JsonPrimitive(((IntField)field).getValue());
		case STRING_TYPE:
			return new JsonPrimitive(((StringField)field).getValue());
		}
		return null;
	}
}
