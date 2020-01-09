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
public class HelloTest {

	@GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {

    	return "hello";
    }
    
}