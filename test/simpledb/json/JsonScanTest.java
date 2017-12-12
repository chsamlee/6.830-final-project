package simpledb.json;

import org.junit.*;

import static org.junit.Assert.*;

import java.io.*;
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
    public void readTest() throws Exception {
    	SeqScan ss = new SeqScan(tid, Database.getCatalog().getTableId("students"));
    	System.out.println(ss.getTupleDesc());
    	Tuple t = ss.next();
		System.out.println(t);
    }

    @Test
    public void easyTest() {
    	String jsonStr = "{ firstname: 'John',"
		    	   + "  lastname: 'Doe',"
		    	   + "  school: 3,"
		    	   + "  year: 1"
		    	   + "}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void abbreviatedFieldNamesTest() {
    	String jsonStr = "{ first: 'John',"
    					+"	last: 'Doe',"
    					+"	year: 1,"
    					+"	s_id: 3"
				 		+"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    	JsonScan scan = new JsonScan(new TransactionId(), object, 0);
    	try {
    		Tuple next = scan.next();
    	} catch(Exception e)
    	{
    		e.printStackTrace();
    		fail();
    	}
    }

    @Test
    public void missingFieldsTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lastname: 'Doe',"
    	 +"year: 1"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void stringToIntTest() {
    	String jsonStr = "{ firstname: 'John',"
    			+"lastname: 'Doe',"
    	 +"year: '1st'"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void intToStringTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lastname: 3,"
    	 +"year: 2,"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void extraFieldsTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lastname: 'Doe',"
    	 +"year: 1,"
    	 +"middle: 'Batman',"
    	 +"school: 1"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void sameFielddTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lname: 'Glenn',"
    	 +"last: 'Doe',"
    	 +"year: 1,"
    	 +"school: 1"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void nestedNewObjectTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lastname: 'Doe',"
    	 +"year: 1,"
    	 +"middle: 'Batman',"
    	 +"school: {"
    	 	+"name: 'CMU',"
    	 	+"city: {"
    	 		+"name: 'Pittsburgh',"
	 			+"state: 'PA'"
 			+"}"
    	 +"}"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void nestedExistingObjectTest() {
    	String jsonStr = "{ firstname: 'John',"
    	 +"lastname: 'Doe',"
    	 +"year: 1,"
    	 +"school: {"
    	 	+"name: 'MIT'"
    	 +"}"
    	 +"}";
    	JsonParser parser = new JsonParser();
    	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
    }

    @Test
    public void arrayOfObjectsTest() {
    	String jsonStr = "[{"
    	 +"name: 'University of Arizona',"
    	 +"city: {"
    	 +	"name: 'Tucson',"
    	 +"		state: 'AZ'"
    	 +"  }"
    	 +" },{"
    	 +"	name: 'Georgia Tech',"
    	 +"	city: {"
    	 +"		name: 'Atlanta',"
    	 +"		state: 'GA'"
    	 +"  }"
    	 +" },{"
    	 +"	name: 'Emory',"
    	 +"	city: {"
    	 +"		name: 'Atlanta',"
    	 +"		state: 'GA'"
    	 +"  }"
    	 +" },{"
    	 +"	name: 'Stanford',"
    	 +"	city: {"
    	 +"		name: 'Stanford',"
    	 +"		state: 'CA'"
    	 +"  }"
    	 +" },{"
    	 +"	name: 'Caltech',"
    	 +"	city: {"
    	 +"		name: 'Pasadena',"
    	 +"		state: 'CA'"
    	 +"  }"
    	 +" },"
    	 +" ]";
    	JsonParser parser = new JsonParser();
    	JsonArray array = parser.parse(jsonStr).getAsJsonArray();
    }

}
