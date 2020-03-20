package com.dexels.navajo.document.stream;

import com.dexels.immutable.api.ImmutableTypeParser;
import com.dexels.navajo.document.*;
import com.dexels.navajo.document.stream.api.Method;
import com.dexels.navajo.document.stream.api.*;
import com.dexels.navajo.document.stream.api.Prop.Direction;
import com.dexels.navajo.document.stream.events.Events;
import com.dexels.navajo.document.stream.events.NavajoStreamEvent;
import com.dexels.navajo.document.types.Binary;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.util.*;
import java.util.stream.Collectors;

import static com.dexels.navajo.document.stream.events.Events.*;

public class NavajoDomStreamer {

	
	private final static Logger logger = LoggerFactory.getLogger(NavajoDomStreamer.class);

	private NavajoDomStreamer() {
	}
	public static Observable<NavajoStreamEvent> feed(final Navajo navajo) {
		return Observable.fromIterable(processNavajo(navajo));
	}
	
	public static Flowable<NavajoStreamEvent> feedFlowable(final Navajo navajo) {
		return Flowable.fromIterable(processNavajo(navajo));
	}
	
	public static List<NavajoStreamEvent> processNavajo(Navajo navajo) {
		List<NavajoStreamEvent> result = new ArrayList<>();
		Navajo output = NavajoFactory.getInstance().createNavajo();
		List<Message> all = navajo.getAllMessages();
		Header h = navajo.getHeader();
		if(h!=null) {
			result.add(header(h));
		} else {
			logger.warn("Unexpected case: Deal with tml without header?");
		}
		for (Message message : all) {
			emitMessage(message,result,output);
		}
		NavajoStreamEvent done = done(navajo.getAllMethods().stream().map(e->new Method(e.getName())).collect(Collectors.toList()));
		result.add(done);
		return result;
	}
	public static Flowable<NavajoStreamEvent> streamMessage(Message message) {
		List<NavajoStreamEvent> result = new ArrayList<>();
		Navajo output = NavajoFactory.getInstance().createNavajo();
		emitMessage(message,result,output);
		return Flowable.fromIterable(result);
		
	}
	
	// TODO extract async and piggyback attributes
	// TODO extract locale header
	private static NavajoStreamEvent header(Header h) {
		return Events.started(new NavajoHead(h.getRPCName(), Optional.ofNullable(h.getRPCUser()),Optional.ofNullable( h.getRPCPassword()), h.getHeaderAttributes(),Collections.emptyMap(),Collections.emptyMap(),Collections.emptyMap()));
	}
	

	private static void emitMessage(Message message,List<NavajoStreamEvent> list, Navajo outputNavajo) {
//		String path = getPath(message);
		String name = message.getName();
		Map<String,Object> messageAttributes = getMessageAttributes(message);
		if(message.isArrayMessage()) {
			list.add(arrayStarted(name,messageAttributes));
			Message definition = message.getDefinitionMessage();
			if(definition!=null) {
				String definitionname =name+"@definition";
				list.add(messageDefinitionStarted(definitionname));
				list.add(Events.messageDefinition(messageDefinition(definition), definitionname));
			}
			for (Message m : message.getElements()) {
				Map<String,Object> elementAttributes = getMessageAttributes(message);
				list.add(arrayElementStarted(elementAttributes));
				for (Message sm : m.getAllMessages()) {
					emitMessage(sm, list,outputNavajo);
				}
				emitBinaryProperties(m,list);
				list.add(Events.arrayElement(messageElement(m),elementAttributes));
			}
			list.add(arrayDone(name));
		} else {
			list.add(messageStarted(name,messageAttributes));
			for (Message m : message.getAllMessages()) {
				emitMessage(m, list,outputNavajo);
			}
			emitBinaryProperties(message,list);
			list.add(Events.message( message(message), name,messageAttributes));
		}

	}

	private static void emitBinaryProperties(Message m, List<NavajoStreamEvent> list) {
		for(Property p: m.getAllProperties()) {
			if(Property.BINARY_PROPERTY.equals(p.getType())) {
				list.add(Events.binaryStarted(p.getName(),p.getLength(),Optional.ofNullable(p.getDescription()),Optional.ofNullable(p.getDirection()),Optional.ofNullable(p.getSubType())));
				Binary b = (Binary) p.getTypedValue();
				if(b!=null) {
					try {
						b.writeBase64(new Writer(){

							@Override
							public void write(char[] cbuf, int off, int len) throws IOException {
								list.add(Events.binaryContent( new String(cbuf,off,len)));
							}

							@Override
							public void flush() throws IOException {
							}

							@Override
							public void close() throws IOException {
							}});
					} catch (IOException e) {
						logger.error("Error: ", e);
					}
				}
				list.add(Events.binaryDone());
			}
		}
		
	}
	private static Map<String, Object> getMessageAttributes(Message message) {
		Map<String,Object> attr = new HashMap<>();
		if(message.getMode()!=null && !"".equals(message.getMode())) {
			attr.put(Message.MSG_MODE, message.getMode());
		}
		if(message.getEtag()!=null && !"".equals(message.getEtag())) {
			attr.put(Message.MSG_ETAG, message.getEtag());
		}
		return attr;
	}

	private static List<Prop> messageProperties(Message msg) {
		List<Property> all = msg.getAllProperties();
		final List<Prop> result = new ArrayList<>();
		for (Property property : all) {
			result.add(create(property));
		}
		return  Collections.unmodifiableList(result);
	}

	private static Msg message(Message tmlMessage) {
		return Msg.create(messageProperties(tmlMessage));
	}

	private static Msg messageDefinition(Message tmlMessage) {
		return Msg.createDefinition(messageProperties(tmlMessage));
	}

	private static Msg messageElement(Message tmlMessage) {
		return Msg.createElement(messageProperties(tmlMessage));
	}

	private static Prop create(Property tmlProperty) {
		String type = tmlProperty.getType();
		
		List<Select> selections = selectFromTml(tmlProperty.getAllSelections());
		String value = null;
		Binary binary = null;
		if(Property.BINARY_PROPERTY.equals(type)) {
			value = null;
			binary = (Binary) tmlProperty.getTypedValue();
		} else if (Property.SELECTION_PROPERTY.equals(type)) {
			value = null;
		} else {
			value = tmlProperty.getValue();
		}
		Optional<Direction> direction = "in".equals(tmlProperty.getDirection())?Optional.of(Direction.IN):"out".equals(tmlProperty.getDirection())?Optional.of(Direction.OUT):Optional.empty();
		return Prop.create(tmlProperty.getName(),value, ImmutableTypeParser.parseType(tmlProperty.getType()),selections,direction, tmlProperty.getDescription(),tmlProperty.getLength(),tmlProperty.getSubType(),Optional.ofNullable(tmlProperty.getCardinality()),binary);
	}
	
	 private static List<Select> selectFromTml(List<Selection> in) {
		 if(in==null) {
			 return Collections.emptyList();
		 }
		 List<Select> result = new ArrayList<>();
		 for (Selection selection : in) {
			result.add(Select.create(selection.getName(), selection.getValue(), selection.isSelected()));
		}
		 return result;
	 }

//	public static Msg create(Message)
	
}
