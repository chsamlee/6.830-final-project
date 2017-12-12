package simpledb.json;

import org.junit.*;

import static org.junit.Assert.*;

import java.io.*;
import java.util.NoSuchElementException;

import com.google.gson.*;

import simpledb.*;

public class JsonScanTest {

    private static final Catalog catalog = Database.getCatalog();
	private static TransactionId tid;

	@Before
    public void setUp() {
	    tid = new TransactionId();
        for (SampleData.Table t : SampleData.tables) {
            try {
                HeapFile hf = Utility.createHeapFile(t.data, t.name + ".dat", t.td);
                catalog.addTable(hf, t.name, "id");
            }
            catch (IOException ioe) {
                ioe.printStackTrace();
            }
            catch (SchemaException se) {
                // most likely due to bad sample data
                se.printStackTrace();
                System.exit(1);
            }
        }
        catalog.addForeignKey(catalog.getTableId("students"), 4, catalog.getTableId("schools"));
        catalog.addForeignKey(catalog.getTableId("schools"), 2, catalog.getTableId("cities"));
    }

    @After
    public void tearDown() {
	    for (SampleData.Table t : SampleData.tables) {
	        pageCleanup(t.name);
	        pageCleanup("$" + t.name + "_extras");
        }
        catalog.clear();
    }

    private void pageCleanup(String name) {
        // discard the pages
        HeapFile hf = (HeapFile) catalog.getDatabaseFile(catalog.getTableId(name));
        for (int pg = 0; pg < hf.numPages(); pg++) {
            Database.getBufferPool().discardPage(new HeapPageId(hf.getId(), pg));
        }
        // physically remove the file
        File f = new File(name + ".dat");
        boolean fileDeleted = f.delete();
        if (!fileDeleted) {
            System.out.println("Warning: file " + f + "not cleaned up properly");
        }
    }

    @Test
    public void easyTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 5,"
    			   + "firstname: 'John',"
		    	   + "  lastname: 'Doe',"
		    	   + "  school: 3,"
		    	   + "  year: 2"
		    	   + "}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(5));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(2));
		assertEquals(record.getField(4), new IntField(3));
    }

    @Test
    public void abbreviatedFieldNamesTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 19,"
    					+"  first: 'John',"
    					+"	last: 'Doe',"
    					+"	year: 1,"
    					+"	s_id: 3"
				 		+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(19));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(3));
    }

    @Test
    public void missingFieldsTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 42,"
    			+ "firstname: 'John',"
	    	    +"lastname: 'Doe',"
	    	    +"year: 1"
	    	    +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(42));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(0));
	}

    @Test
    public void stringToIntTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 13,"
    					+"  firstname: 'John',"
    					+"  lastname: 'Doe',"
				   	    +"  year: '1st',"
				   	    +"  school: 4"
				    	+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(13));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(4));
    }

    @Test
    public void intToStringTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 18,"
    					+"  firstname: 'John',"
			    	    +"  lastname: 3,"
			    	    +"  year: 2,"
				        +"  school: 4"
			        	+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(18));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("3", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(2));
		assertEquals(record.getField(4), new IntField(4));
    }

    @Test
    public void extraFieldsTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 6,"
    				    +"  firstname: 'John',"
			    	    +"  lastname: 'Doe',"
			    	    +"  year: 1,"
			    	    +"  middle: 'Batman',"
			    	    +"  school: 1"
			    	    +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(6));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(1));
    }

    @Test
    public void sameFieldTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 7,"
    					+"  firstname: 'John',"
		    	 		+"  l: 'Glenn',"
		    	 		+"  last: 3,"
		    	 		+"  year: 1,"
		    	 		+"  school: 1"
		    	 		+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(7));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Glenn", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(1));
    }

    @Test
    public void nestedNewObjectTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "{ id: 6,"
					   	+"  firstname: 'John',"
					   	+"  lastname: 'Doe',"
					   	+"  year: 1,"
					   	+"  middle: 'Batman',"
					   	+"  school: {"
					   	+"		id: 5,"
					   	+"  	name: 'CMU',"
					   	+"		city: {"
					   	+"			id: 4,"
					   	+"			city: 'Pittsburgh',"
					   	+"			state: 'PA'"
					   	+"		}"
					   	+"	}"
					   	+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, catalog.getTableId("students"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(4));
		assertEquals(record.getField(1), new StringField("Pittsburgh", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("PA", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(5));
		assertEquals(record.getField(1), new StringField("CMU", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(4));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(6));
		assertEquals(record.getField(1), new StringField("John", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("Doe", Type.STRING_LEN));
		assertEquals(record.getField(3), new IntField(1));
		assertEquals(record.getField(4), new IntField(5));
    }

    @Test
    public void arrayOfObjectsTest() throws NoSuchElementException, DbException, TransactionAbortedException {
    	String jsonStr = "[{ id: 3,"
    					+"   name: 'University of Arizona',"
    					+"   city: {"
    					+"      id: 9,"
    					+"      city: 'Tucson',"
    					+"		state: 'AZ'"
    					+"   }"
    					+" },{"
    					+"   id: 4,"
    					+"	 name: 'Georgia Tech',"
    					+"	 city: {"
    					+"      id: 10,"
    					+"		city: 'Atlanta',"
    					+"		state: 'GA'"
    					+"   }"
    					+" },{"
    					+"   id: 5,"
    					+"	 name: 'Emory',"
    					+"	 city: {"
    					+"      id: 10,"
    					+"		city: 'Atlanta',"
    					+"		state: 'GA'"
    					+"   }"
    					+" },{"
    					+"   id: 6,"
    					+"	 name: 'Stanford',"
    					+"	 city: {"
    					+"      id: 11,"
    					+"		city: 'Stanford',"
    					+"		state: 'CA'"
    					+"   }"
    					+" },{"
    					+"   id: 7,"
    					+"	 name: 'Caltech',"
    					+"	 city: {"
    					+"      id: 12,"
    					+"		city: 'Pasadena',"
    					+"		state: 'CA'"
    					+"   }"
    					+" }]";
    	JsonParser parser = new JsonParser();
    	JsonArray array = parser.parse(jsonStr).getAsJsonArray();
    	JsonScan scan = new JsonScan(new TransactionId(), array, catalog.getTableId("schools"));
		Tuple record = scan.next();
		assertEquals(record.getField(0), new IntField(9));
		assertEquals(record.getField(1), new StringField("Tucson", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("AZ", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(3));
		assertEquals(record.getField(1), new StringField("University of Arizona", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(9));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(10));
		assertEquals(record.getField(1), new StringField("Atlanta", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("GA", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(4));
		assertEquals(record.getField(1), new StringField("Georgia Tech", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(10));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(10));
		assertEquals(record.getField(1), new StringField("Atlanta", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("GA", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(5));
		assertEquals(record.getField(1), new StringField("Emory", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(10));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(11));
		assertEquals(record.getField(1), new StringField("Stanford", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("CA", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(6));
		assertEquals(record.getField(1), new StringField("Stanford", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(11));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(12));
		assertEquals(record.getField(1), new StringField("Pasadena", Type.STRING_LEN));
		assertEquals(record.getField(2), new StringField("CA", Type.STRING_LEN));
		record = scan.next();
		assertEquals(record.getField(0), new IntField(7));
		assertEquals(record.getField(1), new StringField("Caltech", Type.STRING_LEN));
		assertEquals(record.getField(2), new IntField(12));
    }

}
