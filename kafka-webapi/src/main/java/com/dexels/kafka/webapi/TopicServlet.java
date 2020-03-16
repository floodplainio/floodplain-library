package com.dexels.kafka.webapi;

import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.dexels.replication.api.ReplicationMessage;
import com.dexels.replication.api.ReplicationMessageParser;
import com.dexels.replication.factory.ReplicationFactory;
import com.dexels.replication.impl.json.JSONDumpReplicationMessageParserImpl;
import com.dexels.replication.impl.json.JSONReplicationMessageParserImpl;
import com.dexels.replication.impl.protobuf.FallbackReplicationMessageParser;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Flowable;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.reactivestreams.servlet.ResponseSubscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.AsyncContext;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Component(configurationPolicy=ConfigurationPolicy.IGNORE,  name="dexels.kafka.topic.servlet",property={"servlet-name=dexels.kafka.topic.servlet","alias=/topic","asyncSupported.Boolean=true"},immediate=true,service = { Servlet.class})
public class TopicServlet extends HttpServlet implements Servlet {

	private static final long serialVersionUID = 8563181635935834994L;
	private PersistentSubscriber persistentSubscriber;
	private PersistentPublisher publisher;


	
	private final static Logger logger = LoggerFactory.getLogger(TopicServlet.class);

	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	
	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearPersistentSubscriber")
	public void setPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = persistenSubscriber;
	}
	
	public void clearPersistentSubscriber(PersistentSubscriber persistenSubscriber) {
		this.persistentSubscriber = null;
	}

	@Reference(policy=ReferencePolicy.DYNAMIC, unbind="clearTopicPublisher")
	public void setTopicPublisher(PersistentPublisher publisher) {
		this.publisher = publisher;
	}
	
	
	public void clearTopicPublisher(TopicPublisher publisher) {
		this.publisher = null;
	}

	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String command = req.getParameter("cmd");
		resp.addHeader("Access-Control-Allow-Origin", "*");
		boolean doList = req.getParameter("all")!=null;
		if(doList) {
			downloadList(resp,true);
			return;
		}
		boolean generations = req.getParameter("generations")!=null;
		if(generations) {
			downloadList(resp,false);			
		}
		
		if(command==null) {
			downloadTopic(req, resp);
			return;
		}
		switch (command) {
			case "delete":
				KafkaTools.deleteGenerationTopics(publisher, resp.getWriter(), req.getParameter("tenant"), req.getParameter("deployment"), req.getParameter("generation"));
				return;
//			case "deleteAll":
//				deleteDeploymentTenant(req, resp, req.getParameter("tenant"), req.getParameter("deployment"));
//				return;
			case "redelete":
				String topic = req.getParameter("topic");
				publishDeletes(topic);
			case "mockup":
				mockup();
		}
	}
	
	

	private void mockup() throws IOException {
		publisher.create("TESTCLUB",Optional.of(1),Optional.of(1));
		publisher.create("TESTCLUBADDRESS",Optional.of(1),Optional.of(1));
		publisher.create("TESTADDRESS",Optional.of(1),Optional.of(1));
		
		ReplicationMessage club = ReplicationFactory.standardMessage(ImmutableFactory.empty().with("organizationid", 1, "integer"))
				.withPrimaryKeys(Arrays.asList(new String[]{"organizationid"}))
				;
		ReplicationMessage address = ReplicationFactory.standardMessage(ImmutableFactory.empty().with("addressid", 10, "integer")
				.with("street", "monkey", "string")
				)
				.withPrimaryKeys(Arrays.asList(new String[]{"addressid"}))
				;
		
		ReplicationMessage otheraddress = ReplicationFactory.standardMessage(ImmutableFactory.empty().with("addressid", 10, "integer")
				.with("street", "othermonkey", "string")
				)
				.withPrimaryKeys(Arrays.asList(new String[]{"addressid"}))
				;

		
		ReplicationMessage otheraddress2 = ReplicationFactory.standardMessage(ImmutableFactory.empty().with("addressid", 10, "integer")
				.with("street", "pemguin", "string")
				)
				.withPrimaryKeys(Arrays.asList(new String[]{"addressid"}))
				;
		
		ReplicationMessage clubaddress = ReplicationFactory.standardMessage(ImmutableFactory.empty().with("organizationid", 1, "integer")
				.with("addressid", 10, "integer")
				).withPrimaryKeys(Arrays.asList(new String[]{"addressid","organizationid"}))
				;
//		publisher.publish("MOCKUP-test-TESTCLUB", "1", ReplicationFactory.getInstance().serialize(club));
//		publisher.publish("MOCKUP-test-TESTCLUBADDRESS", "10<$>1", ReplicationFactory.getInstance().serialize(clubaddress));
//		publisher.publish("MOCKUP-test-TESTADDRESS", "10", ReplicationFactory.getInstance().serialize(address));
		publisher.publish("MOCKUP-test-TESTADDRESS", "10", ReplicationFactory.getInstance().serialize(otheraddress2));
		publisher.flush();
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}

	private void downloadTopic(HttpServletRequest req, HttpServletResponse resp) throws IOException, ServletException {
		ReplicationMessageParser jsonparser = null;

		List<String> accept = Collections.list(req.getHeaders("Accept"));
		String topic = req.getParameter("topic");
		if(topic==null) {
			resp.sendError(400, "Topic parameter required");
			return;
		}
		Optional<Long> count = Optional.ofNullable(req.getParameter("count")).map(e->Long.parseLong(e));
		
		if(accept.contains("application/json")) {
			resp.setCharacterEncoding("UTF-8");
			resp.setContentType("application/json");
			jsonparser = new JSONReplicationMessageParserImpl();

		} else if(accept.contains("application/x-ndjson")) {
			resp.setCharacterEncoding("UTF-8");
			resp.setContentType("application/x-ndjson");
			jsonparser = new JSONDumpReplicationMessageParserImpl();
		}
		Optional<String> from = Optional.ofNullable(req.getParameter("from"));
		Optional<String> to = Optional.ofNullable(req.getParameter("to"));
		AsyncContext ac = req.startAsync();
		ac.setTimeout(1000000);
		ResponseSubscriber responseSubscriber = new ResponseSubscriber(ac);

		ReplicationMessageParser parser = new FallbackReplicationMessageParser(true);
		Flowable<PubSubMessage> messageFlow = TopicDump.downloadTopicRaw(persistentSubscriber,publisher, topic,from , to);
		
		if(count.isPresent()) {
			messageFlow = messageFlow.take(count.get());
		}

		if(accept.contains("application/json") || accept.contains("application/x-ndjson")) {
			messageFlow
				.map(parser::parseBytes)
				.map(jsonparser::serialize)
				.map(ByteBuffer::wrap)
				.subscribe(responseSubscriber);
		} else {
			messageFlow
				.map(e->e.value())
				.map(ByteBuffer::wrap)
				.subscribe(responseSubscriber);
		}
		
			
	}
	
	private void publishDeletes(String topic) throws IOException {
		Files.lines(Paths.get("/"))
			.forEach(e->{try {
				publishDelete(topic, e);
			} catch (Exception e1) {
				logger.error("Error: ", e1);
			}});
	}
	private void publishDelete(String topic,String key) throws IOException {
		
		this.publisher.publish(topic, key, null);
		this.publisher.flush();
		logger.info("Published delete to topic: {} and key: {}",topic,key);
	}
	
	private void downloadList(HttpServletResponse resp, boolean everything) throws IOException, ServletException {
		
		TopicStructure struct = KafkaTools.getTopics(publisher).blockingGet();

		try {
			objectMapper.writerWithDefaultPrettyPrinter().writeValue(resp.getWriter(), struct);
		} catch (JsonGenerationException e1) {
			throw new ServletException("Error forming json", e1);
		} catch (JsonMappingException e1) {
			throw new ServletException("Error forming json", e1);
		}
	
	}
}
