package com.dexels.kafka.streams.api;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.factory.ReplicationFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.dexels.immutable.api.ImmutableMessage.ValueType;

public class CoreOperators {
	private static final int TOPIC_PARTITION_COUNT = 1;
	private static final int TOPIC_REPLICATION_COUNT = 1;

	private static ObjectMapper objectMapper = new ObjectMapper();
	private static ObjectWriter objectWriter = objectMapper.writer().withDefaultPrettyPrinter();

	private static final Logger logger = LoggerFactory.getLogger(CoreOperators.class);

	private CoreOperators() {}
	
	public static int topicPartitionCount() {
		String env = System.getenv("TOPIC_PARTITION_COUNT");
		if(env!=null) {
			return Integer.valueOf(env);
		}
		return TOPIC_PARTITION_COUNT;
	}

	public static int topicReplicationCount() {
		String env = System.getenv("TOPIC_REPLICATION_COUNT");
		if(env!=null) {
			return Integer.valueOf(env);
		}
		return TOPIC_REPLICATION_COUNT;
	}
	
	public static List<ReplicationMessage> addToReplicationList(List<ReplicationMessage> a, List<ReplicationMessage> b, int maxListSize, BiFunction<ReplicationMessage, ReplicationMessage, Boolean> equalityFunction) {
		List<ReplicationMessage> result = new LinkedList<>(a);
		
		for (ReplicationMessage otherMsg : b) {
		    
		
		    if(result.size()>=maxListSize) {
                logger.warn("List join overflowing max size: {}, ignoring key: {} message:\n{}",maxListSize,otherMsg.queueKey(),new String(otherMsg.toBytes(ReplicationFactory.getInstance())));
                break;
            }
		    
		    for (ReplicationMessage listMsg : a) {
		        final Boolean equalMatched = equalityFunction.apply(listMsg, otherMsg);
				if (equalMatched) {
                    result.remove(listMsg);
                    break;
                }
	        }
            result.add(otherMsg);
		}
		return Collections.unmodifiableList(result);
	}
	public static List<ReplicationMessage> removeFromReplicationList(List<ReplicationMessage> a, List<ReplicationMessage> b) {
		return removeFromReplicationList(a, b,(x,y)->x.equals(y));
	}
	
	public static List<ReplicationMessage> removeFromReplicationList(List<ReplicationMessage> a, List<ReplicationMessage> b, BiFunction<ReplicationMessage, ReplicationMessage, Boolean> equalityFunction) {
		if(a==null) {
			return Collections.emptyList();
		}
		List<ReplicationMessage> result = new LinkedList<>();
		for (ReplicationMessage listMsg : a) {
		    boolean contains = false;
		    for (ReplicationMessage otherMsg : b) {
		        
	            if (equalityFunction.apply(listMsg, otherMsg)) {
	                contains = true;
	                break;
	            }
	        }
		    
		    if (!contains) {
		        result.add(listMsg);
		    }
		    
		}

		return Collections.unmodifiableList(result);
	}


	public static byte[] writeObjectValue(ObjectNode node) {
		try {
			return objectWriter.writeValueAsBytes(node);
		} catch (JsonProcessingException e) {
			logger.error("Error: ", e);
			return new byte[]{};
		}
		
	}

	public static Map<String,String> transformerParametersFromTopicName(String topicName) {
		String[] parts = topicName.split("-");
		Map<String,String> result = new HashMap<>();
		result.put("tenant", parts[0]);
		return Collections.unmodifiableMap(result);
	}
	public static ReplicationMessage joinReplication(ReplicationMessage core, List<ReplicationMessage> added, String sub, List<String> ignore) {
		if(added!=null) {
			core = core.withSubMessages(sub, added.stream().map(m->m.message().without(ignore)).collect(Collectors.toList()));
		}
		return core;
	}
	
	public static List<ReplicationMessage> joinMultiReplication(List<ReplicationMessage> core, ReplicationMessage added, Optional<String> sub, Optional<List<String>> onlyTheseColumns) {
		if(added!=null) {
			if (sub.isPresent()) {
				final ReplicationMessage addToCore = onlyTheseColumns.isPresent() ? added .withOnlyColumns(onlyTheseColumns.get()) : added;
				return core.stream().map(msg->msg.withSubMessage(sub.get(), addToCore.message())).collect(Collectors.toList());
			} else {
				return core.stream().map(msg->msg.merge(added, onlyTheseColumns)).collect(Collectors.toList());
			}
		}
		return core;
	}


	public static ReplicationMessage joinReplicationSingle(ReplicationMessage core, ReplicationMessage added, Optional<String> sub,Optional<List<String>> onlyTheseColumns) {
		if(core==null) {
			logger.warn("No core. Thought this didn't happen");
			return null;
		}
		
		if(added==null) {
			logger.debug("No added. ignoring");
			return core;
		}
//		logger.debug("Merging core: "+core.toString()+" to added: "+added.toString());
		if(sub.isPresent()) {
			if(onlyTheseColumns.isPresent()) {
				// TODO: Unit test or this case
				return core.withSubMessage(sub.get(), added.withOnlyColumns(onlyTheseColumns.get()).message());
			} else {
				return core.withSubMessage(sub.get(), added.message());
			}
		} else {
			return core.merge(added,onlyTheseColumns);
		}
	}
	
	
	
	public static String extractKey(ReplicationMessage msg, String keyDefinition) {
		if(msg==null) {
			throw new NullPointerException("Can not extract key from null message with key definition: "+keyDefinition);
		}
		String[] keys = keyDefinition.split(",");
		if(keys.length==1) {
			return String.valueOf(msg.columnValue(keys[0]));
		}
		return Arrays.asList(keys).stream().map(e->String.valueOf(msg.columnValue(e))).collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
	}
	public static ReplicationMessage merge(String key, ReplicationMessage a, ReplicationMessage b) {
		if(b==null) {
			return a;
		}
		return ReplicationFactory.joinReplicationMessage(key, a, b);
	}


	public static ReplicationMessage joinFieldList(ReplicationMessage a, List<ReplicationMessage> joinList, String keyField, String valueField, List<String> ignore,Optional<String> subField) {
		if(joinList==null) {
			if(ignore.isEmpty()) {
				return a;
			}
		}
		ReplicationMessage current = a;
		if(subField.isPresent()) {
			Optional<ImmutableMessage> sub = current.subMessage(subField.get());
			if(!sub.isPresent()) {
				current = current.withSubMessage(subField.get(), current.message());
			}
			for (ReplicationMessage replicationMessage : joinList) {
				String key = (String) replicationMessage.columnValue(keyField);
				if(key!=null) {
					key = key.toLowerCase(); // seems to be the standard
					Object val = replicationMessage.columnValue(valueField);
					ValueType type = replicationMessage.columnType(valueField);
					if(val!=null) {
						current = current.with(key, val, type);
					}
					current = current.without(ignore);
				}
			}
		} else {
			for (ReplicationMessage replicationMessage : joinList) {
				String key = (String) replicationMessage.columnValue(keyField);
				if(key!=null) {
					key = key.toLowerCase(); // seems to be the standard
					Object val = replicationMessage.columnValue(valueField);
					ValueType type = replicationMessage.columnType(valueField);
					if(val!=null) {
						current = current.with(key, val, type);
					}
					current = current.without(ignore);
				}
			}
		}
		return current.without(ignore);
	}
	

    public static String topicName(String topicName,TopologyContext context) {
    	if(topicName.contains("-generation-")) {
    		logger.warn("Warning: Re-resolving topic: {}",topicName);
    		Thread.dumpStack();
    	}
    	String topic = topicNameForReal(topicName, context);
    	if(topic.indexOf('@')!=-1) {
    		throw new UnsupportedOperationException("Bad topic: "+topic+" from instance: "+context.instance+" tenant: "+context.tenant+" deployment: "+context.deployment+" generation: "+context.generation);
    	}
    	return topic;
    }
    
    private static String topicNameForReal(String topicName,TopologyContext context) {
    	
    	String name;
		name = topicName;

    	
    	if(name==null) {
    		throw new NullPointerException("Can not create topic name when name is null. tenant: "+context.tenant.orElse("<no tenant>") +" deployment: "+context.deployment+" generation: "+context.generation);
    	}
    	
    	if(name.startsWith("@")) {
    		StringBuffer sb = new StringBuffer();
    		if(context.tenant.isPresent()) {
    			sb.append(context.tenant.get()+"-");
    		}
    		String[] withInstance = name.split(":");
    		if (withInstance.length>1) {
    			sb.append(context.deployment+"-"+context.generation+"-"+withInstance[0].substring(1)+"-"+withInstance[1]);
    			throw new IllegalArgumentException("Instance / generational references are no longer supported");
			} else {
    			sb.append(context.deployment+"-"+context.generation+"-"+context.instance+"-"+name.substring(1));
			}
    		return sb.toString();
    	}
    	if (context.tenant.isPresent()) {
    		return context.tenant.get()+"-"+context.deployment+"-"+name;
		} else {
			return context.deployment+"-"+name;
		}
	}

    public static String resolveGenerations(String name, TopologyContext context) {
    	if(name.startsWith("@")) {
    		String[] withInstance = name.split(":");
    		if (context.tenant.isPresent()) {
        		if (withInstance.length>1) {
            		return context.tenant.get()+"-"+context.deployment+"-"+context.generation+"-"+withInstance[0].substring(1)+"-"+withInstance[1];
    			} else {
    	    		return context.tenant.get()+"-"+context.deployment+"-"+context.generation+"-"+context.instance+"-"+name.substring(1);
    			}
			} else {
	    		if (withInstance.length>1) {
	        		return context.deployment+"-"+context.generation+"-"+withInstance[0].substring(1)+"-"+withInstance[1];
				} else {
		    		return context.deployment+"-"+context.generation+"-"+context.instance+"-"+name.substring(1);
				}
			}
//    		return tenant+"-"+deployment+"-"+generation+"-"+instance+"-"+name.substring(1);
    	}
    	return name;
    }
    public static String generationalGroup(String name, TopologyContext context) {
    	if(name.startsWith("@")) {
    		String[] withInstance = name.split(":");
    		if (context.tenant.isPresent()) {
        		if (withInstance.length>1) {
            		return context.tenant.get()+"-"+context.deployment+"-"+context.generation+"-"+withInstance[0].substring(1)+"-"+withInstance[1];
    			} else {
    	    		return context.tenant.get()+"-"+context.deployment+"-"+context.generation+"-"+context.instance+"-"+name.substring(1);
    			}
			} else {
	    		if (withInstance.length>1) {
	        		return context.deployment+"-"+context.generation+"-"+withInstance[0].substring(1)+"-"+withInstance[1];
				} else {
		    		return context.deployment+"-"+context.generation+"-"+context.instance+"-"+name.substring(1);
				}
			}
//    		return tenant+"-"+deployment+"-"+generation+"-"+instance+"-"+name.substring(1);
    	}
    	if(context.tenant.isPresent()) {
    		return context.tenant.get()+"-"+context.deployment+"-"+context.generation+"-"+context.instance+"-"+name;
    	} else {
    		return context.deployment+"-"+context.generation+"-"+context.instance+"-"+name;
    	}
	}

	public static  Function<ReplicationMessage,String> extractKey(String keyDefinition) {
		if(keyDefinition.indexOf(',')==-1) {
			return  (msg)->""+msg.columnValue(keyDefinition);
		}
		final List<String> parts = Arrays.asList(keyDefinition.split(","));
		return  (msg)->{
			return parts.stream().map(e->""+msg.columnValue(e)).collect(Collectors.joining(ReplicationMessage.KEYSEPARATOR));
		};
	}
	
	
	public static BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> getJoinFunction(Optional<String> into, Optional<String> columns, boolean reverse) {
		BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> res = getJoinFunction(into, columns);
		if (reverse) {
			return (m1,m2)->res.apply(m2, m1);
		} else {
			return res;
		}
	}


	public static BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> getParamJoinFunction() {
		return (a,b)->a.withParamMessage(b.message());
	}
	public static BiFunction<ReplicationMessage, ReplicationMessage, ReplicationMessage> getJoinFunction(Optional<String> into, Optional<String> columns) {
		if (into.isPresent()) {
			if (columns.isPresent()) {
				List<String> parsedColumns = Arrays.asList(columns.get().split(","));
				return (original,added)->original.withSubMessage(into.get(),added.withOnlyColumns(parsedColumns).message());				
			} else {
				return (original,added)->original.withSubMessage(into.get(),added.message());
			}
		} else {
			if (columns.isPresent()) {
				List<String> parsedColumns = Arrays.asList(columns.get().split(","));
				return (original,added)->original.merge(added,Optional.of(parsedColumns));				
			} else {
				return (original,added)->original.merge(added,Optional.empty());				
			}
		}
	}
	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> getListJoinFunction(String into,boolean skipEmpty, Optional<String> columns) {
	    return (message,list)-> {
	        if (list.isEmpty() && skipEmpty) {
	            return message;
	        } else {
	            List<ImmutableMessage> withList;
	            if (!columns.isPresent()) {
	                withList = list.stream().map(e->e.message()).collect(Collectors.toList());
	            } else {
	                List<String> parsedColumns = Arrays.asList(columns.get().split(","));
	                withList = list.stream().map(e->e.message().withOnlyColumns(parsedColumns)).collect(Collectors.toList());
	            }
	            return message.withSubMessages(into, withList);
	        }
	    };
	}

	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> getListJoinFunctionFirstOnly(String into) {
		return (message,list)->list.isEmpty()?message:message.withSubMessage(into, list.stream().findFirst().get().message());
	}

	public static BiFunction<ReplicationMessage, List<ReplicationMessage>, ReplicationMessage> getListJoinFunctionLastOnly(String into) {
		return (message,list)->list.isEmpty()?message:message.withSubMessage(into, list.stream().
				reduce((a,b)->b)
				.get().message());
	}


	
	public static String ungroupKey(String key) {
		int index = key.lastIndexOf('|');
		if(index==-1) {
			return key;
		}
		return key.substring(index+1, key.length());
	}
	
	public static String ungroupKeyReverse(String key) {
        int index = key.indexOf('|');
        if(index==-1) {
            return key;
        }
        return key.substring(0, index);
    }



}
