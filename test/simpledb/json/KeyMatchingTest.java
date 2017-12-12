package simpledb.json;

import org.junit.Test;
import com.google.gson.*;
import static org.junit.Assert.*;

import simpledb.Type;

public class KeyMatchingTest {

	@Test
	public void testExactMatch() {
    	String jsonStr = "{ firstname: 'John',"
			    	   + "  lastname: 'Doe',"
			    	   + "  school: 3,"
			    	   + "  year: 1"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("year", Type.INT_TYPE);
	 	assertEquals(new JsonPrimitive(1), match);
	}

	@Test
	public void testInexactMatch() {
    	String jsonStr = "{ first_name: 'John',"
			    	   + "  lastname: 'Doe',"
			    	   + "  school: 3,"
			    	   + "  year: 1"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("firstName", Type.INT_TYPE);
	 	assertEquals(new JsonPrimitive("John"), match);
	}

	@Test
	public void testPrefix() {
    	String jsonStr = "{ first_name: 'John',"
			    	   + "  lastname: 'Doe',"
			    	   + "  s: 3,"
			    	   + "  year: 1"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("school", Type.INT_TYPE);
	 	assertEquals(new JsonPrimitive(3), match);
	}

	@Test
	public void testIdSuffix() {
    	String jsonStr = "{ first_name: 'John',"
			    	   + "  lastname: 'Doe',"
			    	   + "  s_id: 3,"
			    	   + "  year: 1"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("school", Type.INT_TYPE);
	 	assertEquals(new JsonPrimitive(3), match);
	}

	@Test
	public void testMultipleOfDifferentTypes() {
    	String jsonStr = "{ sch: 'MIT',"
			    	   + "  s: 4"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("school", Type.INT_TYPE);
	 	assertEquals(new JsonPrimitive(4), match);
	 	match = matching.getMatchingKey("school", Type.STRING_TYPE);
	 	assertEquals(new JsonPrimitive("MIT"), match);
	}

	@Test
	public void testNestedValues() {
    	String jsonStr = "{ school: {"
			       	   + "	  name: 'MIT',"
			   		   + "	  city: 'Cambridge'"
					   + "	},"
			    	   + "  name: 'Jeremy'"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("city", Type.STRING_TYPE);
	 	assertEquals(new JsonPrimitive("MIT"), match.getAsJsonObject().get("name"));
	}
	
	@Test
	public void testNoMatch() {
    	String jsonStr = "{"
			   		   + "  id: 5,"
					   + "  city: 'Cambridge',"
			    	   + "  name: 'Jeremy'"
			    	   + "}";
	 	JsonParser parser = new JsonParser();
	 	JsonObject object = parser.parse(jsonStr).getAsJsonObject();
	 	KeyMatching matching = new KeyMatching(object);
	 	JsonElement match = matching.getMatchingKey("entry", Type.INT_TYPE);
	 	assertNull(match);
	}
}