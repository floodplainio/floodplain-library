package com.dexels.kafka.streams.sink.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Binary;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableTypeParser;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.replication.api.ReplicationMessage;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

import io.reactivex.functions.Action;

public class MongoTuple {
	public final List<ReplicationMessage> in;
	public final List<WriteModel<BsonDocument>> models;
	public final Action commit;
	
	public MongoTuple(List<ReplicationMessage> in) {
		Map<String,Long> offsetMap = new HashMap<>();
		Map<String,ReplicationMessage> toCommit = new HashMap<>();
//		for (ReplicationMessage replicationMessage : in) {
//			System.err.println("Topic: "+replicationMessage.source()+" partition: "+replicationMessage.partition()+" offset: "+replicationMessage.offset());
//		}
		in.stream().filter(e->e.source().isPresent() && e.partition().isPresent() && e.offset().isPresent()).forEach(msg->{
			String path = msg.source().get()+"/"+msg.partition().get();
			long offset = msg.offset().get();
			Long existing = offsetMap.get(path);
			if(existing==null || offset > existing) {
				offsetMap.put(path, offset);
				toCommit.put(path, msg);
			}
		});
		System.err.println("Offset: "+offsetMap);
		int sz = in.size();
		this.models = in.stream().map(di->toBson(di.message())).map(e->new InsertOneModel<BsonDocument>(e)).collect(Collectors.toList());
		this.commit = ()->{
			toCommit.values().forEach(e->{
				System.err.println("Committing: "+offsetMap+" -> "+toCommit.keySet());
				e.commit();
			});
		};
		this.in = in;
	}
	
	public static BsonDocument toBson(ImmutableMessage im) {
		BsonDocument result = new BsonDocument();
		im.columnNames().forEach(e->{
			Object value = im.columnValue(e);
			BsonValue val = createBsonFromObject(value, im.columnType(e));
			result.put(e, val);
		});
		im.subMessageMap().entrySet().forEach(e->{
			String key = e.getKey();
			result.put(key, toBson(e.getValue()));
		});
		im.subMessageListMap().entrySet().forEach(e->{
			String key = e.getKey();
			result.put(key, toBson(e.getValue()));
		});
		return result;
	}

	public static BsonArray toBson(List<ImmutableMessage> im) {
		BsonArray result = new BsonArray();
		im.stream().map(e->toBson(e)).forEach(doc->result.add(doc));
		return result;
	}
	
	public static BsonValue createBsonFromObject(Object value, String typeString) {
		if(value==null) {
			return new BsonNull();
		}
		ValueType type = ImmutableTypeParser.parseType(typeString);
		switch(type) {
		case STRING:
			return new BsonString((String) value);
		case BOOLEAN:
			return new BsonBoolean((Boolean)value);
		case FLOAT:
			return new BsonDouble((Double)value);
		case INTEGER:
			return new BsonInt32((Integer)value);
		case LONG:
			return new BsonInt64((Long)value);
		case LIST:
			List<String> parts = (List<String>) value;
			return new BsonArray(parts.stream().map(BsonString::new).collect(Collectors.toList()));

		case BINARY:
			Binary b = (Binary) value;
			return new BsonBinary(b.getData());
		default:
			throw new UnsupportedOperationException("Can convert this type: "+type+" to BSON. Contributions welcome");
		}
	}

	
}
