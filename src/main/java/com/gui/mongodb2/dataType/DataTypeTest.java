package com.gui.mongodb2.dataType;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class DataTypeTest {

	@SuppressWarnings("unused")
	private static void test1() {
		MongoClient client = null;
		try {
			client = new MongoClient("192.168.0.242", 27001);
		} catch (UnknownHostException e) {
			System.err.println("error when connect");
			return;
		}
		
		DBCollection collection = client.getDB("core_sql").getCollection("drug_tender_result");
		DBCursor cursor = collection.find(new BasicDBObject().append("region_en", "Shanghai"));
		
		int i = 0;
		for(DBObject obj : cursor) {
			if (obj.get("price_per_unit") != null) {
				System.out.println(obj.get("price_per_unit").getClass());
				System.out.println(obj.get("price_per_unit"));
			}
			++i;
			if (i == 100) {
				break;
			}
		}
		client.close();
	}
	
	public static void main(String[] args) {
	//	test1();
		Double d = 0.000447;
		System.out.println(d);
	}
}
