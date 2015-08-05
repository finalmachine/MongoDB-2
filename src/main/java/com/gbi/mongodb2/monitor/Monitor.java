package com.gbi.mongodb2.monitor;

import java.io.Closeable;
import java.net.UnknownHostException;

import com.gbi.commons.config.Params;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class Monitor implements Closeable {
	
	public static final String suffix = ".log";

	private String host;
	private int port;
	private String database;
	private String collection;
	private String collection_log;

	private MongoClient client = null;
	private DBCollection collection1 = null;
	private DBCollection collection2 = null;

	private int create;
	private int insert;
	private int delete;
	private int update;

	public Monitor(String host, int port, String database, String collection) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.collection = collection;
		this.collection_log = collection + suffix;
	}
	
	public Monitor(String host, int port, String database, String collection, String collection_log) {
		this.host = host;
		this.port = port;
		this.database = database;
		this.collection = collection;
		this.collection_log = collection_log;
	}

	public void open() throws UnknownHostException {
		client = new MongoClient(host, port);
		DB db = client.getDB(database);
		collection1 = db.getCollection(collection);
		collection2 = db.getCollection(collection_log);
	}

	public long log() {
		if (client == null || collection1 == null || collection2 == null) {
			throw new RuntimeException("没有调用run函数");
		}

		create = 0;
		insert = 0;
		delete = 0;
		update = 0;
		long time = System.currentTimeMillis();

		BasicDBObject ref = new BasicDBObject();
		BasicDBObject key = new BasicDBObject();
		key.put("_id", 1);

		// go through collection2
		DBCursor cursor = collection2.find(ref, key);
		while (cursor.hasNext()) {
			DBObject id2 = (DBObject) cursor.next();
			if (collection1.findOne(id2) == null) {// collection1 not have the
													// id
				DBObject log = collection2.findOne(id2);
				if (log.get("image") != null) {// not have been removed
					remove(time, log);
				}
			}
		}

		// go through collection1
		cursor = collection1.find();
		while (cursor.hasNext()) {
			DBObject currentEntity = cursor.next();
			Object id = currentEntity.get("_id");
			ref.put("_id", id);
			DBObject log = collection2.findOne(ref);
			if (log != null) { // 有一个同样id的
				if (log.get("image") == null) {// 在回收站
					insert2(id, time, currentEntity, log);
				} else {// 不在回收站
					DBObject image = (DBObject) log.get("image");
					DBObject detail = new BasicDBObject();
					for (String currentKey : currentEntity.keySet()) {
						if (image.containsField(currentKey)) {
							if (!image.get(currentKey).equals(currentEntity.get(currentKey))) {// change
								detail.put(currentKey, image.get(currentKey));
							}
						} else {// add
							detail.put(currentKey, null);
						}
					}
					for (String currentKey : image.keySet()) { // delete
						if (!currentEntity.containsField(currentKey)) {
							detail.put(currentKey, image.get(currentKey));
						}
					}
					change(time, currentEntity, log, detail);
				}
			} else { // 肯定没有这个数据
				insert1(id, time, currentEntity);
			}
		}
		return time;
	}

	public void insert1(final Object id, final long time, final DBObject currentObj) {
		DBObject newLog = new BasicDBObject();
		// 准备一条log开始
		newLog.put("_id", id);
		newLog.put("image", currentObj);
		// 准备history列表开始
		BasicDBList history = new BasicDBList();
		history.add(insert_prepare_option(time, currentObj));
		// 准备history列表完成
		newLog.put("history", history);
		// 准备一条log结束
		collection2.insert(newLog);

		++create;
	}

	public void insert2(final Object id, final long time, final DBObject currentObj, DBObject log) {
		BasicDBList history = (BasicDBList) log.get("history");
		// update history >
		history.add(insert_prepare_option(time, currentObj));
		// update history <
		log.put("history", history);
		// update image >
		log.put("image", currentObj);
		// update image <
		collection2.save(log);

		++insert;
	}

	private DBObject insert_prepare_option(final long time, final DBObject currentObj) {
		DBObject option = new BasicDBObject();
		// 准备一条option >
		option.put("time", time);
		option.put("option", "create");
		option.put("detail", currentObj);
		// 准备一条option <
		return option;
	}

	private void remove(long time, DBObject log) {
		// prepare history >
		BasicDBList history = (BasicDBList) log.get("history");
		DBObject option = new BasicDBObject();
		option.put("time", time);
		option.put("option", "delete");
		option.put("detail", log.get("image"));
		history.add(option);
		// prepare history <
		log.put("history", history);
		log.put("image", null);

		collection2.save(log);
		++delete;
	}

	private void change(long time, final DBObject currentObj, DBObject log, DBObject detail) {
		if (detail.keySet().size() > 0) {
			// update history >
			BasicDBList history = (BasicDBList) log.get("history");
			// prepare option >
			DBObject option = new BasicDBObject();
			option.put("time", time);
			option.put("option", "update");
			option.put("detail", detail);
			// prepare option <
			history.add(option);
			// update history <
			log.put("history", history);
			log.put("image", currentObj);
			collection2.save(log);
			++update;
		}
	}
	
	public void recovery(long time) {
		if (client == null || collection1 == null || collection2 == null) {
			throw new RuntimeException("没有调用run函数");
		}
		DBCursor cursor = collection2.find();
		for (DBObject log : cursor) {
			DBObject now = (DBObject) log.get("image");
			BasicDBList history = (BasicDBList) log.get("history");
			boolean changed = false;
			for (int i = history.size() - 1; i >= 0; --i) {
				DBObject option = (DBObject) history.get(i);
				if ((long) option.get("time") <= time) {
					break;
				} else {
					switch ((String) option.get("option")) {
					case "create":
						now = new BasicDBObject();
						break;
					case "delete":
						now = (DBObject) option.get("detail");
						break;
					case "update":
						DBObject detail = (DBObject) option.get("detail");
						for (String key : detail.keySet()) {
							Object obj = detail.get(key);
							if (obj == null) {
								now.removeField(key);
							} else {
								now.put(key, obj);
							}
						}
						break;
					default:
						System.out.println("有一个错误的命令操作:" + option.get("option"));
						break;
					}
					changed = true;
				}
			}
			if (changed) {
				if (now.keySet().size() > 0) {
					collection1.save(now);
				} else {
					collection1.remove(new BasicDBObject("_id", log.get("_id")));
				}
			}
		}
		System.out.println("recovery finish");
	}

	@Override
	public void close() {
		client.close();
	}

	public static void main(String[] args) {
		Monitor m = new Monitor(Params.MongoDB.TEST.host, Params.MongoDB.TEST.port, Params.MongoDB.TEST.database,
				"proxy");
		try {
			m.open();
			m.recovery(1438759258836L);
			m.close();
		} catch (UnknownHostException e) {
			System.err.println("地址有问题");
		}
	}

	public int getCreate() {
		return create;
	}

	public int getInsert() {
		return insert;
	}

	public int getDelete() {
		return delete;
	}

	public int getUpdate() {
		return update;
	}
}
