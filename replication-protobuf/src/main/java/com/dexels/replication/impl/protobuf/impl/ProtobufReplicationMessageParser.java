package com.dexels.replication.impl.protobuf.impl;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.customtypes.CoordinateType;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessage.Operation;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageListProtobuf;
import com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageProtobuf;
import com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageProtobuf.Builder;
import com.dexels.replication.impl.protobuf.generated.Replication.ValueProtobuf;
import com.dexels.replication.impl.protobuf.generated.Replication.ValueProtobuf.ValueType;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Named;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

@Named("protobuf") @ApplicationScoped
public class ProtobufReplicationMessageParser implements ReplicationMessageParser {

	
	private static final Logger logger = LoggerFactory.getLogger(ProtobufReplicationMessageParser.class);

	public static final int MAGIC = 12779;
	public static final byte MAGIC_BYTE_1 = 8;
	public static final byte MAGIC_BYTE_2 = -21;

	static class ValueTuple {
		public final String key;
		public final ValueProtobuf value;

		public ValueTuple(String key,ValueProtobuf val) {
			this.key = key;
			this.value = val;
		}
	}
	
	private static final SimpleDateFormat dateFormat() {
        return new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SS");
	}

	private static final SimpleDateFormat clocktimeFormat() {
        return new SimpleDateFormat("HH:mm:ss");
	}


	private static String serializeValue(ValueType type, Object val, SimpleDateFormat dateFormat, SimpleDateFormat clocktimeFormat) {
		if(val==null) {
			return null;
		}
		switch (type) {
			case STRING:
				return (String)val;
			case INTEGER:
				return Integer.toString((Integer)val);
			case LONG:
				return Long.toString((Long)val);
			case DOUBLE:
				return Double.toString((Double)val);
			case FLOAT:
				if(val instanceof Float) {
					return Float.toString((Float)val);
				}
				if(val instanceof Double) {
					return Double.toString((Double)val);
				}
			case BOOLEAN:
				return Boolean.toString((Boolean)val);
			case BINARY_DIGEST:
				return (String) val;
			case BINARY:

				System.err.println("Binary type: "+val.getClass());
				return  (String)val;
			case DATE:
				if(val instanceof String) {
					return (String) val;
				}
				return dateFormat.format((Date)val);
			case CLOCKTIME:
				if(val instanceof String) {
					return (String) val;
				}
				return clocktimeFormat.format((Date)val);
			case LIST:
				List<String> v = (List<String>)val;
				return v.stream().collect(Collectors.joining(","));
			case COORDINATE:
				return val.toString();
			case ENUM:
				return val.toString();
			default:
				throw new UnsupportedOperationException("Unknown type: "+type);
		}	
	}
//	"2017-03-31 11:28:46.00"
	private static Object protobufValue(ValueProtobuf val, SimpleDateFormat dataFormat, SimpleDateFormat clockTimeFormat) {
		if(val.getIsNull()) {
			return null;
		}
		String value = val.getValue();
		switch (val.getType()) {
		case STRING:
			return value;
		case INTEGER:
			return Integer.parseInt(value);
		case LONG:
			return Long.parseLong(value);
		case DOUBLE:
			return Double.parseDouble(value);
		case FLOAT:
			return Float.parseFloat(value);
		case BOOLEAN:
			return Boolean.parseBoolean(value);
		case BINARY:
			return val.getByteData()==null? new byte[]{} : val.getByteData().toByteArray();
		case BINARY_DIGEST:
			return value;
		case DATE:
			try {
				return dataFormat.parse(value);
			} catch (ParseException e) {
				logger.warn("Error parsing date: "+value+" with type: "+val.getType().name(),e);
				return null;
			}
		case CLOCKTIME:
			try {
				return clockTimeFormat.parse(value);
			} catch (ParseException e) {
				logger.warn("Error parsing date: "+value+" with type: "+val.getType().name(),e);
				return null;
			}
		case LIST:
			return value;
        case COORDINATE:
            try {
                return new CoordinateType(value);
            } catch (Exception e) {
                logger.warn("Error parsing coordinate: " + value, e);
                return null;
            }
		case ENUM:
			return value;
		default:
			return null;
		}	
	}

	private static ValueProtobuf.ValueType parseType(ImmutableMessage.ValueType type) {
		switch (type) {
			case STRING:
				return ValueType.STRING;
			case INTEGER:
				return ValueType.INTEGER;
			case LONG:
				return ValueType.LONG;
			case DOUBLE:
				return ValueType.DOUBLE;
			case FLOAT:
				return ValueType.FLOAT;
			case BOOLEAN:
				return ValueType.BOOLEAN;
			case BINARY_DIGEST:
				return ValueType.BINARY_DIGEST;
			case DATE:
				return ValueType.DATE;
			case LIST:
				return ValueType.LIST;
			case BINARY:
				return ValueType.BINARY;
			case COORDINATE:
				return ValueType.COORDINATE;
			case CLOCKTIME:
				return ValueType.CLOCKTIME;
			case ENUM:
				return ValueType.ENUM;
			case STOPWATCHTIME:
			case IMMUTABLE:
			case UNKNOWN:
			case IMMUTABLELIST:
			case POINT:
			case REACTIVE:
			case REACTIVESCRIPT:
			case REACTIVEPIPE:
			case REACTIVEPARTIALPIPE:
			case MAPPER:
				return ValueType.UNRECOGNIZED;
		}
		return ValueType.UNRECOGNIZED;
	}
	public static ImmutableMessage.ValueType convertType(ValueType type) {
		switch (type) {

			case STRING:
				return ImmutableMessage.ValueType.STRING;
			case INTEGER:
				return ImmutableMessage.ValueType.INTEGER;
			case LONG:
				return ImmutableMessage.ValueType.LONG;
			case DOUBLE:
				return ImmutableMessage.ValueType.DOUBLE;
			case FLOAT:
				return ImmutableMessage.ValueType.FLOAT;
			case BOOLEAN:
				return ImmutableMessage.ValueType.BOOLEAN;
			case BINARY_DIGEST:
				return ImmutableMessage.ValueType.BINARY_DIGEST;
			case DATE:
				return ImmutableMessage.ValueType.DATE;
			case CLOCKTIME:
				return ImmutableMessage.ValueType.CLOCKTIME;
			case LIST:
				return ImmutableMessage.ValueType.LIST;
			case BINARY:
				return ImmutableMessage.ValueType.BINARY;
			case COORDINATE:
				return ImmutableMessage.ValueType.COORDINATE;
			case ENUM:
				return ImmutableMessage.ValueType.ENUM;
			case UNRECOGNIZED:
				return ImmutableMessage.ValueType.UNKNOWN;
		}
		return ImmutableMessage.ValueType.UNKNOWN;
	}
	public static ImmutableMessage parseImmutableMessage(ReplicationMessageProtobuf source, SimpleDateFormat dataFormat, SimpleDateFormat clockTimeFormat) {
		return parseImmutableMessage(source,true,dataFormat,clockTimeFormat);
	}
	public static ImmutableMessage parseImmutableMessage(ReplicationMessageProtobuf source,boolean checkMagic, SimpleDateFormat dateFormat, SimpleDateFormat clockTimeFormat ) {
		Map<String,Object> values = new HashMap<>();
		Map<String,ImmutableMessage.ValueType> types = new HashMap<>();

		if(checkMagic && ProtobufReplicationMessageParser.MAGIC != source.getMagic()) {
			throw new IllegalArgumentException("Bad magic: "+source.getMagic());
		}
		
		for (Entry<String, ValueProtobuf> b :  source.getValuesMap().entrySet()) {
			types.put(b.getKey(), convertType(b.getValue().getType()));
			values.put(b.getKey(), protobufValue( b.getValue(),dateFormat,clockTimeFormat));
		}
		Map<String,ImmutableMessage> submessage = new HashMap<>();
		for (Entry<String,ReplicationMessageProtobuf> elt : source.getSubmessageMap().entrySet()) {
			submessage.put(elt.getKey(), parseImmutableMessage(elt.getValue(),dateFormat,clockTimeFormat));
		}
		Map<String,List<ImmutableMessage>> submessagelist = new HashMap<>();
		for (Entry<String, ReplicationMessageListProtobuf> elt : source.getSubmessageListMap().entrySet()) {
			List<ImmutableMessage> rm = elt.getValue().getElementsList().stream().map(r->parseImmutableMessage(r,dateFormat,clockTimeFormat)).collect(Collectors.toList());
			submessagelist.put(elt.getKey(), rm);
		}
		return ImmutableFactory.create(values, types, submessage, submessagelist);
		
	}
	public static ReplicationMessage parse(Optional<String> topicSrc, ReplicationMessageProtobuf source, Optional<Runnable> commitAction, SimpleDateFormat dateFormat, SimpleDateFormat clockTimeFormat) {
		Map<String,Object> values = new HashMap<>();
		Map<String, ImmutableMessage.ValueType> types = new HashMap<>();

		if(ProtobufReplicationMessageParser.MAGIC != source.getMagic()) {
			throw new IllegalArgumentException("Bad magic");
		}
		for (Entry<String, ValueProtobuf> b :  source.getValuesMap().entrySet()) {
			types.put(b.getKey(), convertType(b.getValue().getType()));
			values.put(b.getKey(), protobufValue( b.getValue(),dateFormat,clockTimeFormat));
		}
		Map<String,ImmutableMessage> submessage = new HashMap<>();
		for (Entry<String,ReplicationMessageProtobuf> elt : source.getSubmessageMap().entrySet()) {
			submessage.put(elt.getKey(), parseImmutableMessage(elt.getValue(),dateFormat,clockTimeFormat));
		}
		Map<String,List<ImmutableMessage>> submessagelist = new HashMap<>();
		for (Entry<String, ReplicationMessageListProtobuf> elt : source.getSubmessageListMap().entrySet()) {
			List<ImmutableMessage> rm = elt.getValue().getElementsList().stream().map(r->parseImmutableMessage(r,dateFormat,clockTimeFormat)).collect(Collectors.toList());
			submessagelist.put(elt.getKey(), rm);
		}
		Optional<ImmutableMessage> paramMsg =  Optional.ofNullable(source.getParamMessage()).map(msg->parseImmutableMessage(msg,false,dateFormat,clockTimeFormat));
		return ReplicationFactory.createReplicationMessage(topicSrc,Optional.empty(),Optional.empty(), source.getTransactionId(), source.getTimestamp(), Operation.valueOf(source.getOperation().name()), source.getPrimarykeysList().stream().collect(Collectors.toList()), types, values, submessage, submessagelist, commitAction,paramMsg);
	}
	
	@Override
	public byte[] serialize(ReplicationMessage m) {
		final byte[] byteArray = toProto(m).toByteArray();
		if((short)byteArray[0] != ProtobufReplicationMessageParser.MAGIC_BYTE_1) {
			throw new IllegalArgumentException("Bad magic byte: "+(short)byteArray[0]);
		}
		if((short)byteArray[1] != ProtobufReplicationMessageParser.MAGIC_BYTE_2) {
			throw new IllegalArgumentException("Bad magic byte"+(short)byteArray[1]);
		}
		return byteArray;
	}
	
	@Override
	public String describe(ReplicationMessage m) {
		return toProto(m).toString();
	}
	

	private static ReplicationMessageProtobuf toProto(ReplicationMessage msg) {
		return toProto(msg.message(),msg.transactionId(),msg.operation(), msg.timestamp(),msg.primaryKeys(),msg.paramMessage());
	}	
	private static ReplicationMessageProtobuf toProto(ImmutableMessage msg) {
		return toProto(msg,null,Operation.NONE,-1,Collections.emptyList(),Optional.empty());
	}
	private static ReplicationMessageProtobuf toProto(ImmutableMessage msg, String transactionId, Operation operation,  long timestamp, List<String> primaryKeys, Optional<ImmutableMessage> paramMessage) {
		SimpleDateFormat dataFormat = dateFormat();
		SimpleDateFormat clockTimeFormat = clocktimeFormat();

		Map<String,ValueProtobuf> val = msg.values().entrySet()
			.stream()
			.map(e->{
				final Object value = msg.columnValue(e.getKey());
				final ImmutableMessage.ValueType type = msg.types().getOrDefault(e.getKey(),ImmutableMessage.ValueType.STRING);
				ValueType parseType = parseType(type);

				if (parseType == ValueType.UNRECOGNIZED) {
				    logger.warn("Unknown type for key {}, value {}, type {}", e.getKey(), value, type);
				    if (type.equals("null")) {
				        parseType = ValueProtobuf.ValueType.STRING;
	                }
				}
				if(parseType.equals(ValueType.BINARY)) {
					if(value!=null) {
						ByteString bs = ByteString.copyFrom((byte[])value);
						return new ValueTuple(e.getKey(),ValueProtobuf
									.newBuilder()
									.setType(parseType)
									.setByteData(bs)
									.build()
								);
					} else {
						return new ValueTuple(e.getKey(),ValueProtobuf
								.newBuilder()
								.setType(parseType)
								.setIsNull(true)
								.build()
							);
						
					}
				} else {
					final String serializeValue = serializeValue(parseType(type),value,dataFormat,clockTimeFormat);
					if (serializeValue==null) {
						return new ValueTuple(e.getKey(),ValueProtobuf.newBuilder()
								.setType(parseType)
								.setIsNull(value==null)
								.build()
						);
					} else {
						return new ValueTuple(e.getKey(),ValueProtobuf.newBuilder()
								.setValue(serializeValue)
								.setType(parseType)
								.setIsNull(value==null)
								.build()
						);
					}
				}
		}
		).collect(Collectors.toMap(e->e.key, e->e.value));
		
		Builder b = ReplicationMessageProtobuf.newBuilder()
				.setMagic(ProtobufReplicationMessageParser.MAGIC)
				.addAllPrimarykeys(primaryKeys)
				.setOperation(ReplicationMessageProtobuf.Operation.valueOf(operation.name()))
				.setTimestamp(timestamp)
				.putAllValues(val);
		
		if(transactionId!=null) {
			b = b.setTransactionId(transactionId);
		}
		Map<String,ReplicationMessageProtobuf> subm = new HashMap<>();
		if(!msg.subMessageMap().isEmpty()) {
			for (Entry<String,ImmutableMessage> e : msg.subMessageMap().entrySet()) {
				subm.put(e.getKey(), toProto(e.getValue()));
			} 
		}
		b = b.putAllSubmessage(subm);

		Map<String,ReplicationMessageListProtobuf> subml = new HashMap<>();
		if(!msg.subMessageListMap().isEmpty()) {
			for (Entry<String,List<ImmutableMessage>> e : msg.subMessageListMap().entrySet()) {
				
				final com.dexels.replication.impl.protobuf.generated.Replication.ReplicationMessageListProtobuf.Builder newBuilder = ReplicationMessageListProtobuf.newBuilder();
				newBuilder.addAllElements(e.getValue().stream().map(repl->toProto(repl)).collect(Collectors.toList()));
				subml.put(e.getKey(), newBuilder.build());
				
			}

		}
		if(paramMessage.isPresent()) {
			b.setParamMessage(toProto(paramMessage.get()));
		}
		b = b.putAllSubmessageList(subml);
	    return b.build();
	}

	@Override
	public ReplicationMessage parseBytes(byte[] data) {
		return parseBytes(Optional.empty(), data);
	}
	@Override
	public ReplicationMessage parseBytes(Optional<String> source, byte[] data) {
		if(data==null) {
			return null;
		}
		ReplicationMessageProtobuf parsed;
		try {
			parsed = ReplicationMessageProtobuf.parseFrom(data);
			return parse(source, parsed,Optional.empty(),dateFormat(),clocktimeFormat());
		} catch (InvalidProtocolBufferException e) {
			logger.error("InvalidProtocolBufferException: ", e);
			return ReplicationFactory.createErrorReplicationMessage(e);
		}

	}

	@Override
	public List<ReplicationMessage> parseMessageList(Optional<String> source, byte[] data) {
		SimpleDateFormat dataFormat = dateFormat();
		SimpleDateFormat clockTimeFormat = clocktimeFormat();

		if(data==null) {
			return Collections.emptyList();
		}
		try {
			return ReplicationMessageListProtobuf.parseFrom(data).getElementsList().stream().map(e->parse(source, e,Optional.empty(),dataFormat,clockTimeFormat)).collect(Collectors.toList());
		} catch (InvalidProtocolBufferException e) {
			logger.error("Error invalid: ", e);
			return Arrays.asList(ReplicationFactory.createErrorReplicationMessage(e));
		}
	}

	@Override
	public ReplicationMessage parseStream(Optional<String> source, InputStream data) {
		SimpleDateFormat dataFormat = dateFormat();
		SimpleDateFormat clockTimeFormat = clocktimeFormat();
		try {
			return parse(Optional.empty(), ReplicationMessageProtobuf.parseFrom(data),Optional.empty(),dataFormat,clockTimeFormat);
		} catch (IOException e) {
			logger.error("Error: ", e);
			return ReplicationFactory.createErrorReplicationMessage(e);
		}
	}

	@Override
	public byte[] serializeMessageList(List<ReplicationMessage> msgs) {
		return ReplicationMessageListProtobuf.newBuilder().setMagic(ProtobufReplicationMessageParser.MAGIC).addAllElements(msgs.stream().map(msg->toProto(msg)).collect(Collectors.toList())).build().toByteArray();
	}

	@Override
	public List<ReplicationMessage> parseMessageList(Optional<String> source, InputStream data) {
		SimpleDateFormat dataFormat = dateFormat();
		SimpleDateFormat clockTimeFormat = clocktimeFormat();
		try {
			// make small pushback
			PushbackInputStream pis = new PushbackInputStream(data, 2);
			byte[] pre = new byte[2];
			int i = pis.read(pre);
			System.err.println("i: "+i);
			if((short)pre[0] != ProtobufReplicationMessageParser.MAGIC_BYTE_1) {
				throw new IllegalArgumentException("Bad magic byte: "+(short)pre[0]);
			}
			if((short)pre[1] != ProtobufReplicationMessageParser.MAGIC_BYTE_2) {
				throw new IllegalArgumentException("Bad magic byte"+(short)pre[1]);
			}			
			pis.unread(pre);
			return ReplicationMessageListProtobuf.parseFrom(pis).getElementsList().stream().map(e->parse(Optional.empty(),e,Optional.empty(),dataFormat,clockTimeFormat)).collect(Collectors.toList());
		} catch (IOException e) {
			logger.error("Error: ", e);
			return Arrays.asList(ReplicationFactory.createErrorReplicationMessage(e));
		}
	}
	@Override
	public ReplicationMessage parseStream(InputStream data) {
		return parseStream(Optional.empty(), data);
	}
	@Override
	public List<ReplicationMessage> parseMessageList(byte[] data) {
		return parseMessageList(Optional.empty(), data);
	}
	@Override
	public ReplicationMessage parseBytes(PubSubMessage data) {
		ReplicationMessage initial = (data.value()!=null ? parseBytes(data.value()) : ReplicationFactory.empty().withOperation(Operation.DELETE));
		if(ReplicationMessage.includeKafkaMetadata()) {
			return initial
					.withPartition(data.partition())
					.withOffset(data.offset())
					.withSource(data.topic())
					.with("_kafkapartition", data.partition().orElse(-1), ImmutableMessage.ValueType.INTEGER)
					.with("_kafkaoffset", data.offset().orElse(-1L), ImmutableMessage.ValueType.LONG)
					.with("_kafkakey", data.key(), ImmutableMessage.ValueType.STRING)
					.with("_kafkatopic",data.topic().orElse(null), ImmutableMessage.ValueType.STRING);
		}
		return initial;
	}


}
