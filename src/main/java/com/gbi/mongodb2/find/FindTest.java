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
			client = new MongoClient("192.168.0.242", 27001);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("core");
		DBCollection collection = database.getCollection("institution_basic");
		
		DBObject query = new BasicDBObject();
		query.put("_id", 92015582);

		DBCursor cursor = collection.find(query);
		System.out.println(cursor.count());
		System.out.println(cursor.size());

		client.close();
	}
	
	public static void findAndwrite() {
		MongoClient client = null;
		try {
			client = new MongoClient("192.168.0.252", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("Metrix");
		DBCollection collection1 = database.getCollection("PapersPubmedGrab");
		DBCollection collection2 = database.getCollection("PapersPubmedGrab.need");

		DBObject query = new BasicDBObject();
		query.put("content.Publish_Year", "2015");
		DBObject keys = new BasicDBObject();
		keys.put("content.authorList", 1);
		keys.put("_id", 1);

		DBCursor cursor = collection1.find(query, keys);
	//	System.out.println(cursor.size());\
		int i = 0;
		while (cursor.hasNext()) {
			DBObject element = cursor.next();
			DBObject insert = new BasicDBObject();
			insert.put("_id", element.get("_id"));
			insert.put("authorList", ((DBObject) element.get("content")).get("authorList"));
			System.out.println(insert);
			collection2.insert(insert);
		//	collection2.insert(element);
			System.out.println(++i);
		}
		
		client.close();
	}

	public static void main(String[] args) {
		findAndwrite();
	}
}
