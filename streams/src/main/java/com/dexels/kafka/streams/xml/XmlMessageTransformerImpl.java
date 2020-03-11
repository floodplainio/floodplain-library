package com.dexels.kafka.streams.xml;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.streams.api.CoreOperators;
import com.dexels.kafka.streams.base.StreamOperators;
import com.dexels.kafka.streams.xml.parser.XMLElement;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.transformer.api.MessageTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class XmlMessageTransformerImpl implements MessageTransformer {
	public static final ReplicationMessageParser parser = ReplicationFactory.getInstance();
	private final AtomicLong counter = new AtomicLong();
	private final List<MessageTransformer> subtransformers = new ArrayList<>();
	private final List<String> subTransformerDescriptions = new ArrayList<>();
	private final boolean log;
	private final XMLElement definition;
	
	private final static Logger logger = LoggerFactory.getLogger(XmlMessageTransformerImpl.class);

	
	public XmlMessageTransformerImpl(Map<String,MessageTransformer> transformerRegistry,XMLElement xe, String sourceTopicName) {
		this.definition = xe;
		this.log = "true".equals(xe.getStringAttribute("log"));
		if(this.log) {
			logger.info("logged message found");
		}
		for (XMLElement part : xe.getChildren()) {
			Iterator<String> enumerateAttributeNames = part.enumerateAttributeNames();
			Map<String,String> attributes = new HashMap<>( CoreOperators.transformerParametersFromTopicName(sourceTopicName));
			while (enumerateAttributeNames.hasNext()) {
				String name = (String) enumerateAttributeNames.next();
				attributes.put(name, part.getStringAttribute(name));
			}
			
			switch (part.getName()) {
			case "rename":
				subTransformerDescriptions.add("rename: "+attributes);
				subtransformers.add((p,message)->{
					return message.rename(attributes.get("from"), attributes.get("to"));
				});
				break;
			case "renameSubMessage":
				subTransformerDescriptions.add("renameSubMessage: "+attributes);
				subtransformers.add((p,message)->{
					String field = attributes.get("from");
					String to = attributes.get("to");
					Optional<ImmutableMessage> from = message.subMessage(field);
					if(!from.isPresent()) {
						return message;
					}
					ImmutableMessage src = from.get();
					return message.withoutSubMessage(field).withSubMessage(to, src);
//					message.setSubMessage(field, null);
//					message.setSubMessage(to, from.get());
//					return message;
				});
				break;
			case "renameSubMessages":
				subTransformerDescriptions.add("renameSubMessages: "+attributes);
				subtransformers.add((p,message)->{
					// TODO remove mutability
					String field = attributes.get("from");
					String to = attributes.get("to");
					Optional<List<ImmutableMessage>> from = message.subMessages(field);
					if(!from.isPresent()) {
						return message;
					}
					return message.withoutSubMessages(field).withSubMessages(to,from.get());
				});
				break;
			case "flattenToFirst":
				subTransformerDescriptions.add("flattenToFirst: "+attributes);
				subtransformers.add((p,message)->{
					// TODO remove mutability
					String field = attributes.get("from");
					String to = attributes.get("to");
					Optional<List<ImmutableMessage>> from = message.subMessages(field);
					if(!from.isPresent()) {
						return message;
					}
					if(from.get().isEmpty()) {
						return message;
					}
					ImmutableMessage first = from.get().stream().findFirst().get();
					final ReplicationMessage transformed = message.withoutSubMessages(field).withSubMessage(to, first);
					if(log) {
						logger.info("Flattening to first");
						logMessage(message,transformed);
					}
					return transformed;
				});
				break;
			case "flattenToLast":
				subTransformerDescriptions.add("flattenToFirst: "+attributes);
				subtransformers.add((p,message)->{
					// TODO remove mutability
					String field = attributes.get("from");
					String to = attributes.get("to");
					Optional<List<ImmutableMessage>> from = message.subMessages(field);
					if(!from.isPresent()) {
						return message;
					}
					if(from.get().isEmpty()) {
						return message;
					}
					ImmutableMessage last = from.get().stream().reduce((a,b)->b).orElse(ImmutableFactory.empty());
					return message.withoutSubMessages(field).withSubMessage(to, last);
				});
				break;		
			case "flattenToColumn":
				subTransformerDescriptions.add("flattenToColumn: "+attributes);
				subtransformers.add((p,message)->{
					// TODO remove mutability
					String name = attributes.get("from");
					String to = attributes.get("to");
					String property = attributes.get("property");
					Optional<ImmutableMessage> from = message.subMessage(name);
					if(!from.isPresent()) {
						return message;
					}
					ImmutableMessage mmsg = from.get();
					return message.withoutSubMessage(name).with(to, mmsg.columnValue(property), mmsg.columnType(property));
				});
				break;			
			case "remove":
				subTransformerDescriptions.add("remove: "+attributes);
				subtransformers.add((p,message)->{
					return message==null?null: message.without(attributes.get("name"));
				});
				break;
			case "joinTransformation":
				subTransformerDescriptions.add("joinTransformation: "+attributes);
				Optional<MessageTransformer> joinTransformation = StreamOperators.transformersFromChildren(Optional.of(part), transformerRegistry,sourceTopicName);
				if(joinTransformation.isPresent()) {
					subtransformers.add(joinTransformation.get());
				}
				break;
			default:
				if (part.getName().startsWith("sink.")) {
					logger.info("Messagetransformer ignoring tag: {}, as it seems to be a sink setting",part.getName());
				} else {
					subTransformerDescriptions.add("custom: "+part.getName()+"-"+attributes);
					
					subtransformers.add((p,message)->{
						MessageTransformer messageTransformer = transformerRegistry.get(part.getName());
						if(messageTransformer==null) {
							throw new NullPointerException("No message transformer found: "+part.getName());
						}
						try {
						    return messageTransformer.apply(attributes, message);
						} catch (Throwable t) {
						    logger.error("Exception in transforming! Transformer: {}", part.getName(), t);
						}
						return message;
					});
				}
				break;

			}
		}
	}

	@Override
	public ReplicationMessage apply(Map<String,String> params, ReplicationMessage input) {
		ReplicationMessage msg = input;
		for (MessageTransformer messageTransformer : subtransformers) {
			msg = messageTransformer.apply(params==null?Collections.emptyMap(): params, msg);
		}
		if(this.log) {
			logMessage(input, msg);
		}
		return  msg;
	}

	private void logMessage(ReplicationMessage input,ReplicationMessage output) {
		counter.incrementAndGet();
		if(logger.isInfoEnabled()) {
			logger.info("Applying transformation: \n{}",this.definition);
			for (String description : subTransformerDescriptions) {
				logger.info("->{}",description);
			}
				
			logger.info("Transforming in: {}", output.queueKey());
			logger.info("BEFORE:\n {}",input.toFlatString(ReplicationFactory.getInstance()));
			logger.info("AFTER:\n{}",output.toFlatString(ReplicationFactory.getInstance()));
			logger.info("DONE--------------:\n");
		}
	}

}
