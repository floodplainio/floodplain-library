package io.floodplain;

import javax.enterprise.inject.Default;
import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.config.spi.ConfigSource;

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
    @Inject ImmutableMessageParser messageParser;
//	@Inject ReplicationMessageParser parser;
	@Inject @Named(value = "protobuf") ReplicationMessageParser parser2;
	@Inject @Named(value = "protobuffallback") ReplicationMessageParser parser3;
	@Inject KafkaTopicPublisherConfiguration pubConfig;
	@ConfigProperty(name="io.floodplain.bootstrapServers", defaultValue = "aapaap")
	public String bootstrapHosts;
	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
    	System.err.println("parser2: "+parser2);
    	System.err.println("parser3: "+parser3);
    	System.err.println("Pubonfig: "+pubConfig.bootstrapServers());
    	System.err.println("bootstrapHosts: "+bootstrapHosts);
    	
    	ImmutableMessage msg = ImmutableFactory.empty().with("aap","noot", ImmutableMessage.ValueType.STRING.name());
    	return "aap.type";
    }
    
}