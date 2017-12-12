package simpledb.json;

import org.junit.*;

import static org.junit.Assert.*;

import java.io.*;
import com.google.gson.*;

import simpledb.*;

public class JsonScanTest {
	
	private static TransactionId tid;

	public static void writeData(Object[][] data, File file) throws IOException {
        File dummyFile = new File("dummy.txt");
        try (FileOutputStream stream = new FileOutputStream(dummyFile)) {
        	for(int i = 0;i<data.length;i++) {
        		for(int j = 0; j<data[i].length;j++) {
        			Object datum = data[i][j];
        			if(datum instanceof String) {
        				stream.write(((String)datum).getBytes());
        			}
        			else if(datum instanceof Integer) {
        				stream.write((Integer)datum);
        			}
        			else {
        				System.err.println("Invalid data");
        			}
        			if(j<data[i].length - 1) {
        				stream.write(",".getBytes());
        			}
        		}
    			if(i<data.length - 1) {
    				stream.write(System.lineSeparator().getBytes());
    			}
        	}
        }
        HeapFileEncoder.convert(dummyFile, file, 128*1000, data[0].length);
	}

    @Before
    public void setUpClass() {
    	tid = new TransactionId();
        System.out.println("Called before any test is run");
        try {
        	// Students
            File studentFile = new File("students.dat");
			writeData(SampleData.getStudentData(), studentFile);
			Database.getCatalog().addTable(new HeapFile(studentFile, SampleData.getStudentTupleDesc()), "students");

        	// Schools
            File schoolFile = new File("schools.dat");
			writeData(SampleData.getSchoolData(), schoolFile);
			Database.getCatalog().addTable(new HeapFile(schoolFile, SampleData.getSchoolTupleDesc()), "schools");
        
        	// Cities
            File cityFile = new File("cities.dat");
			writeData(SampleData.getCityData(), cityFile);
			Database.getCatalog().addTable(new HeapFile(cityFile, SampleData.getCityTupleDesc()), "cities");

        } catch (IOException e) {
			e.printStackTrace();
		}
        /**
         * Create table here
         * 1. have an arraylist of data
         * 2. call convert on the arraylist with outFile = temp file
         * 3. add temp file into catalog
         */
        /**
         * Cleanup
         * 1. delete table from catalog
         * 2. delete temp file (may be auto)
         */
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
    
    @After
    public void cleanUp() {
    	Database.getCatalog().clear();
    }
    
}
