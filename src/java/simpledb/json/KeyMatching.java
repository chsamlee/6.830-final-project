package simpledb.json;

import java.util.*;
import java.util.Map.Entry;

import com.google.gson.*;
import simpledb.*;

public class KeyMatching {
	JsonObject object;
	Set<String> usedKeys = new HashSet<String>();
	
	public KeyMatching(JsonObject obj) {
		object = obj;
	}
	
	public JsonElement getMatchingKey(String fieldName, Type fieldType) {
		JsonElement match = keysWithExactName(fieldName, fieldType);
		if(match != null) {
			return match;
		}
		match = keysWithApproximateName(fieldName, fieldType);
		if(match != null) {
			return match;
		}
		return keysWithNestedMatchings(fieldName, fieldType);
	}
	
	public List<Entry<String, JsonElement>> unusedKeyValues() {
		List<Entry<String, JsonElement>> unused = new ArrayList<Entry<String, JsonElement>>();
		for(Entry<String, JsonElement> entry : object.entrySet()) {
			if(!usedKeys.contains(entry.getKey())) {
				unused.add(entry);
			}
		}
		return Collections.unmodifiableList(unused);
	}

	private JsonElement keysWithExactName(String fieldName, Type fieldType) {
		JsonElement element = object.get(fieldName);
		if(!usedKeys.contains(fieldName)) {
			usedKeys.add(fieldName);
			return element;
		}
		else {
			return null;
		}
	}
	private JsonElement keysWithApproximateName(String fieldName, Type fieldType) {
		Set<String> matchingKeys = new HashSet<String>();
		for(Entry<String, JsonElement> entry : object.entrySet()) {
			if(usedKeys.contains(entry.getKey())) {
				continue;
			}
			if(matchesApproximately(fieldName, entry.getKey())) {
				matchingKeys.add(entry.getKey());
			}
		}
		if(matchingKeys.size()==0) {
			return null;
		}
		else if(matchingKeys.size()==1) {
			String matchedKey = matchingKeys.iterator().next();
			usedKeys.add(matchedKey);
			return object.get(matchedKey);
		}
		else {
			for(String key : matchingKeys) {
				if(matchedType(object.get(key)).equals(fieldType)) {
					usedKeys.add(key);
					return object.get(key);
				}
			}
		}
		return null;
	}

	private JsonElement keysWithNestedMatchings(String fieldName, Type fieldType) {
		for(Entry<String, JsonElement> entry : object.entrySet()) {
			if(!usedKeys.contains(entry.getKey()) && entry.getValue().isJsonObject()) {
				JsonObject value = entry.getValue().getAsJsonObject();
				for(Entry<String, JsonElement> nestedEntry : value.entrySet()) {
					if(nestedEntry.getKey().equals(fieldName) && matchedType(nestedEntry.getValue()).equals(fieldType)) {
						usedKeys.add(entry.getKey());
						return value;
					}
				}
			}
		}
		return null;
	}

	private Type matchedType(JsonElement element) {
		return ((element.isJsonPrimitive()) && element.getAsJsonPrimitive().isString())?Type.STRING_TYPE:Type.INT_TYPE;
	}
	
	private static String stripNonAlphanumeric(String key) {
		return key.replaceAll("[^A-Za-z0-9]", "").toLowerCase();
	}
	
	public static boolean matchesApproximately(String a, String b) {
		if(stripNonAlphanumeric(a).startsWith(stripNonAlphanumeric(b))) {
			return true;
		}
		else if(stripNonAlphanumeric(b).endsWith("id") && stripNonAlphanumeric(b).length() > 2) {
			String withoutId = stripNonAlphanumeric(b).substring(0, stripNonAlphanumeric(b).length() - 2);
			if(stripNonAlphanumeric(a).startsWith(withoutId)) {
				return true;
			}
		}
		return false;
	}
}