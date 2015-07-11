package com.gbi.mongodb2.copy;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class CopyTest {
	
	public static void copyCollection1(String collectionName) {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("test");
		DBCollection collection1 = database.getCollection(collectionName);
		DBCollection collection2 = database.getCollection(collectionName + "_copy");
		
		collection2.drop();
		DBCursor cursor = collection1.find();
		while (cursor.hasNext()) {
			DBObject object = (DBObject) cursor.next();
			collection2.insert(object);
		}
		
		client.close();
		System.out.println("copy " + collectionName + " finished");
	}
	
	public static void copyCollection2(String collectionFrom, String collectionTo) {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("test");
		DBCollection collection1 = database.getCollection(collectionFrom);
		DBCollection collection2 = database.getCollection(collectionTo);
		
		collection2.drop();
		DBCursor cursor = collection1.find();
		while (cursor.hasNext()) {
			DBObject object = (DBObject) cursor.next();
			collection2.insert(object);
		}
		
		client.close();
		System.out.println("copy " + collectionFrom + " finished");
	}
	
	public static void main(String[] args) {
		copyCollection2("test_table1", "test_table1_create");
		copyCollection2("test_table1_remove.log", "test_table1_create.log");
	}
}
