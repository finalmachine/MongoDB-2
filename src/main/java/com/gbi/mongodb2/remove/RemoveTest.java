package com.gbi.mongodb2.remove;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class RemoveTest {
	
	public static void removoTest1() {
		MongoClient client = null;
		try {
			client = new MongoClient("127.0.0.1", 27017);
		} catch (UnknownHostException e) {
			System.err.println("can't connect to the server");
			e.printStackTrace();
			return;
		}
		
		DB database = client.getDB("test");
		DBCollection collection = database.getCollection("test_table1_remove");
		
		DBObject ref = new BasicDBObject();
		ref.put("_id", 1);
		
		collection.remove(ref);
		
		client.close();
	}
	
	public static void removoTest2() {
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
		ref.put("Dénomination sociale", "Merck Medication Familiale");
		
	//	collection.remove(ref);
		System.out.println(collection.remove(ref));
		
		client.close();
	}
	
	public static void main(String[] args) {
		removoTest2();
	}
}
