package com.gbi.mongodb2.monitor;

import org.junit.After;
import org.junit.Before;

import com.gbi.commons.config.Params;
import com.gbi.mongodb2.copy.CopyUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class MonitorTest {
	
	private static MongoClient client = null;
	
	private static DBCollection getCollection(String name) {
		return client.getDB(Params.MongoDB.TEST.database).getCollection(name);
	}
	
	@Before
	private static void open() throws Exception {
		client = new MongoClient(Params.MongoDB.TEST.host, Params.MongoDB.TEST.port);
	}
	
	@After
	private static void close() throws Exception {
		client.close();
	}
	
	public static void test_cd() throws Exception {
		String table = "source_cd";
		CopyUtil.copyCollection("source", table);
		CopyUtil.copyCollection("source.log", table + ".log");

		DBCollection c = getCollection(table);
		DBObject people1 = new BasicDBObject();
		people1.put("_id", 4);
		people1.put("name", "尹志平");
		people1.put("age", 20);
		c.save(people1);
		c.remove(new BasicDBObject("_id", 1));
		
		Monitor m = new Monitor(Params.MongoDB.TEST.host, Params.MongoDB.TEST.port, Params.MongoDB.TEST.database,
				table);
		m.open();
		m.log();
		m.close();
		System.out.println("insert:" + m.getInsert());
		System.out.println("create:" + m.getCreate());
		System.out.println("delete:" + m.getDelete());
		System.out.println("change:" + m.getUpdate());
	}
	
	public static void test_cu() throws Exception {
		String table = "source_cu";
		CopyUtil.copyCollection("source", table);
		CopyUtil.copyCollection("source.log", table + ".log");
		
		DBCollection c = getCollection(table);
		DBObject people1 = new BasicDBObject();
		people1.put("_id", 4);
		people1.put("name", "尹志平");
		people1.put("age", 20);
		c.save(people1);
		
		people1.put("age", 22);
		c.save(people1);
		
		Monitor m = new Monitor(Params.MongoDB.TEST.host, Params.MongoDB.TEST.port, Params.MongoDB.TEST.database,
				table);
		m.open();
		m.log();
		m.close();
		System.out.println("insert:" + m.getInsert());
		System.out.println("create:" + m.getCreate());
		System.out.println("delete:" + m.getDelete());
		System.out.println("change:" + m.getUpdate());
	}
	
	public static void test_cud() throws Exception {
		String table = "source_cud";
		CopyUtil.copyCollection("source", table);
		CopyUtil.copyCollection("source.log", table + ".log");
		
		DBCollection c = getCollection(table);
		DBObject people1 = new BasicDBObject();
		people1.put("_id", 4);
		people1.put("name", "尹志平");
		people1.put("age", 20);
		c.save(people1);
		
		people1.put("age", 22);
		c.save(people1);
		
		c.remove(people1);
		
		Monitor m = new Monitor(Params.MongoDB.TEST.host, Params.MongoDB.TEST.port, Params.MongoDB.TEST.database,
				table);
		m.open();
		m.log();
		m.close();
		System.out.println("insert:" + m.getInsert());
		System.out.println("create:" + m.getCreate());
		System.out.println("delete:" + m.getDelete());
		System.out.println("change:" + m.getUpdate());
	}

	public static void main(String[] args) throws Exception {
		open();
		
		test_cud();
		
		close();
	}
}
