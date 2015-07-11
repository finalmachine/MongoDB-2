package com.gbi.mongodb2.save;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class SaveTest {
	
	private static void saveTest(DBObject obj) {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("test");
		DBCollection collection = database.getCollection("test_table1");
		
		collection.save(obj);
		
		client.close();
	}

	public static void main(String[] args) {
		DBObject obj = new BasicDBObject();
		obj.put("_id", 6);
		obj.put("age", 20);
		obj.put("gender", "female");
		obj.put("name", "周芷若");
		
		saveTest(obj);
		
	}
}
