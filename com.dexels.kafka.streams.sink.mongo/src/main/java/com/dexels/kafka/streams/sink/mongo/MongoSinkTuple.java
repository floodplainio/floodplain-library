package com.dexels.kafka.streams.sink.mongo;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Binary;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessage.ValueType;
import com.dexels.immutable.api.ImmutableTypeParser;
import com.dexels.kafka.streams.api.sink.SinkTuple;
import com.dexels.replication.api.ReplicationMessage;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

public class MongoSinkTuple extends SinkTuple<WriteModel<BsonDocument>> {

	public MongoSinkTuple(List<ReplicationMessage> in) {
		super(in, di->{
			BsonDocument res = toBson(di.message());
			res.put("_id", new BsonString(di.combinedKey()));
			return new ReplaceOneModel<>(Filters.eq("_id", di.combinedKey()), res, new ReplaceOptions().upsert(true));
		});
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
		case DOUBLE:
			return new BsonDouble((Double)value);
		case FLOAT:
			return new BsonDouble((Float)value);
		case INTEGER:
			return new BsonInt32((Integer)value);
		case LONG:
			return new BsonInt64((Long)value);
		case LIST:
			List<String> parts = (List<String>) value;
			return new BsonArray(parts.stream().map(BsonString::new).collect(Collectors.toList()));
		case DATE:
			return new BsonDateTime(((Date)value).getTime());
		case CLOCKTIME:
			return new BsonDateTime(((Date)value).getTime());
		case COORDINATE:
			return new BsonDocument("", new BsonString(""));
		case BINARY:
			if(value instanceof Binary) {
				Binary b = (Binary) value;
				return new BsonBinary(b.getData());
			} else {
				byte[] b = (byte[])value;
				return new BsonBinary(b);
			}
		default:
			throw new UnsupportedOperationException("Can convert this type: "+type+" to BSON. Contributions welcome");
		}
	}
}
