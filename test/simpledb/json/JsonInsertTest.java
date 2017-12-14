package simpledb.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import simpledb.Catalog;
import simpledb.Database;
import simpledb.HeapFile;
import simpledb.HeapPageId;
import simpledb.IntField;
import simpledb.SeqScan;
import simpledb.TransactionId;
import simpledb.Tuple;
import simpledb.TupleDesc;
import simpledb.Type;
import simpledb.json.JsonInsert;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class JsonInsertTest {

    private TransactionId tid = new TransactionId();
    private Catalog catalog = Database.getCatalog();

    private int getTupleCount(String name) {
        SeqScan f = new SeqScan(tid, catalog.getTableId(name));
        int count = 0;
        try {
            f.open();
            while (f.hasNext()) {
                f.next();
                count++;
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        }
        catch (Exception e) {
            System.out.println ("Exception : " + e);
        }
        return count;
    }

    @Before
    public void setUp() throws Exception {
        HeapFile hfa = new HeapFile(
                new File("tableA.dat"),
                new TupleDesc(
                        new Type[]{Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"x", "y"}
                )
        );
        catalog.addTable(hfa, "tableA", "x");

        HeapFile hfb = new HeapFile(
                new File("tableB.dat"),
                new TupleDesc(
                        new Type[]{Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"z", "w"}
                )
        );
        catalog.addTable(hfb, "tableB", "z");

        catalog.addForeignKey(
                catalog.getTableId("tableA"), 1,
                catalog.getTableId("tableB")
        );
    }

    @After
    public void tearDown() throws Exception {
        for (String name : new String[]{"tableA", "tableB", "$tableA_extras", "$tableB_extras"}) {
            // discard the pages
            HeapFile hf = (HeapFile) catalog.getDatabaseFile(catalog.getTableId(name));
            for (int pg = 0; pg < hf.numPages(); pg++) {
                Database.getBufferPool().discardPage(new HeapPageId(hf.getId(), pg));
            }
            // physically remove the file
            File f = new File(name + ".dat");
            f.delete();
        }
    }

    @Test
    public void testBasic() {
        JsonArray json = new JsonArray();
        for (int i = 0; i < 3; i++) {
            JsonObject obj = new JsonObject();
            obj.add("z", new JsonPrimitive(i));
            obj.add("w", new JsonPrimitive(i * i + 1));
            json.add(obj);
        }

        JsonInsert insertOp = new JsonInsert(tid, json, catalog.getTableId("tableB"));
        Tuple result = null;
        try {
            insertOp.open();
            result = insertOp.next();
            insertOp.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        assertEquals(3, ((IntField) result.getField(0)).getValue());
        assertEquals(0, getTupleCount("tableA"));
        assertEquals(3, getTupleCount("tableB"));
    }

    @Test
    public void testReference() {
        JsonArray json = new JsonArray();
        for (int i = 0; i < 4; i++) {
            JsonObject obj = new JsonObject();
            obj.add("x", new JsonPrimitive(i));
            JsonObject nestedObj = new JsonObject();
            nestedObj.add("z", new JsonPrimitive(i + 1));
            nestedObj.add("w", new JsonPrimitive(i * i));
            obj.add("y", nestedObj);
            json.add(obj);
        }

        JsonInsert insertOp = new JsonInsert(tid, json, catalog.getTableId("tableA"));
        Tuple result = null;
        try {
            insertOp.open();
            result = insertOp.next();
            insertOp.close();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        assertEquals(4, ((IntField) result.getField(0)).getValue());
        assertEquals(4, getTupleCount("tableA"));
        assertEquals(4, getTupleCount("tableB"));
    }

}
