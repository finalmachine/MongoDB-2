package com.gbi.mongodb2.monitor;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.Set;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * 一个DBMonitor实体只能用于一个MongoDB集群
 * @author leo.li
 */
public class DBMonitor implements Closeable {
	public static final String suffix = ".log";

	private String _host;
	private int _port;
	private MongoClient _client = null;

	DBCollection _collection1 = null;
	DBCollection _collection2 = null;
	private int create;
	private int insert;
	private int delete;
	private int update;

	public DBMonitor(String host, int port) {
		_host = host;
		_port = port;
	}

	public void open() throws UnknownHostException {
		_client = new MongoClient(_host, _port);
	}

	public long log(String DB, String collection) {
		if (_client == null) {
			System.err.println("monitor没有开启，请先调用open函数");
			return -1;
		}
		create = 0;
		insert = 0;
		delete = 0;
		update = 0;

		_collection1 = _client.getDB(DB).getCollection(collection);
		_collection2 = _client.getDB(DB).getCollection(collection + suffix);
		
		long time = System.currentTimeMillis();

		BasicDBObject ref = new BasicDBObject();
		BasicDBObject key = new BasicDBObject();
		key.put("_id", 1);

		// go through collection2
		DBCursor cursor = _collection2.find(ref, key);
		while (cursor.hasNext()) {
			DBObject id2 = (DBObject) cursor.next();
			if (_collection1.findOne(id2) == null) {// collection1 not have the
													// id
				DBObject log = _collection2.findOne(id2);
				if (log.get("image") != null) {// not have been removed
					remove(time, log);
				}
			}
		}

		// go through collection1
		cursor = _collection1.find();
		while (cursor.hasNext()) {
			DBObject currentEntity = cursor.next();
			Object id = currentEntity.get("_id");
			ref.put("_id", id);
			DBObject log = _collection2.findOne(ref);
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
						} else {// 增加了字段
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
		_collection2.insert(newLog);

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
		_collection2.save(log);

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

		_collection2.save(log);
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
			_collection2.save(log);
			++update;
		}
	}
	
	public void recovery(long time, String DB, String collection) {
		if (_client == null) {
			System.err.println("monitor没有开启，请先调用open函数");
			return;
		}
		Set<String> collectionNames = _client.getDB(DB).getCollectionNames();
		if (!collectionNames.contains(collection) || !collectionNames.contains(collection + suffix)) {
			System.err.println("你选择的表不符合还原的条件");
		}
		_collection1 = _client.getDB(DB).getCollection(collection);
		_collection2 = _client.getDB(DB).getCollection(collection + suffix);

		DBCursor cursor = _collection2.find();
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
					_collection1.save(now);
				} else {
					_collection1.remove(new BasicDBObject("_id", log.get("_id")));
				}
			}
		}
		System.out.println("begin log " + collection);
		log(DB, collection);
		System.out.println("recovery finish " + collection);
	}

	@Override
	public void close() {
		_client.close();
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
