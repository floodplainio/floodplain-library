package io.floodplain;

import javax.enterprise.inject.Default;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.factory.ImmutableFactory;
import com.dexels.kafka.api.KafkaTopicPublisherConfiguration;
import com.dexels.kafka.impl.KafkaTopicSubscriber;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.replication.api.ReplicationMessageParser;


@Path("/hello")
public class thing {

//	@Inject @Default PersistentSubscriber subscriber;
	
	@Inject @Named(value = "json") ReplicationMessageParser parser;
	@Inject @Named(value = "protobuf") ReplicationMessageParser parser2;
	@Inject @Named(value = "protobuffallback") ReplicationMessageParser parser3;
	@Inject KafkaTopicPublisherConfiguration pubConfig;
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
    	ImmutableMessage msg = ImmutableFactory.empty().with("aap","noot", ImmutableMessage.ValueType.STRING.name());
    	return messageParser.describe(msg);
    }
    
    @Inject ImmutableMessageParser messageParser;
}