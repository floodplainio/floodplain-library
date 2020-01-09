package io.floodplain;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.dexels.kafka.api.KafkaTopicPublisherConfiguration;
import com.dexels.kafka.streams.base.StreamRuntime;
import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.replication.api.ReplicationMessageParser;


@Path("/hello")
public class Stream {

//	@Inject @Default PersistentSubscriber subscriber;
//	@Inject ReplicationMessageParser parser;
	@Inject @Named(value = "protobuf") ReplicationMessageParser parser2;
	@Inject @Named(value = "protobuffallback") ReplicationMessageParser parser3;
	@Inject KafkaTopicPublisherConfiguration pubConfig;
	@Inject StreamRuntime runtime;
	@Inject RepositoryInstance repository;
	@ConfigProperty(name="io.floodplain.bootstrapServers", defaultValue = "aapaap")
	public String bootstrapHosts;
	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
		System.err.println(">> "+runtime);
    	System.err.println("parser2: "+parser2);
    	System.err.println("parser3: "+parser3);
    	System.err.println("Pubonfig: "+pubConfig.bootstrapServers());
    	System.err.println("bootstrapHosts: "+bootstrapHosts);
    	System.err.println("deployment: "+repository.getDeployment());
    	
    	return "aap.type";
    }
    
}