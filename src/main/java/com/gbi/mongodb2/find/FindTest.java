package com.gbi.mongodb2.find;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class FindTest {

	public static void findTest1() {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("test");
		DBCollection collection = database.getCollection("test_table1_copy");
		
		DBObject ref = new BasicDBObject();
		DBObject key = new BasicDBObject();
		key.put("_id", 1);
		
		DBCursor cursor = collection.find(ref, key);
		while (cursor.hasNext()) {
			DBObject dbObject = (DBObject) cursor.next();
			System.out.println(dbObject);
		}
		
		client.close();
	}
	
	public static void findTest2() {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("france");
		DBCollection collection = database.getCollection("Recherche_avancee3");
		
		DBObject ref = new BasicDBObject();
		DBObject key = new BasicDBObject();
		key.put("Dénomination sociale", "ASTRAZENECA");
		
		DBCursor cursor = collection.find(ref, key);
		System.out.println(cursor.count());
		System.out.println(cursor.size());
		
		client.close();
	}

	public static void main(String[] args) {
		findTest2();
	}
}
