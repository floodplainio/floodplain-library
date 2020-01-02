package io.floodplain;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.dexels.immutable.api.ImmutableMessage;
import com.dexels.immutable.api.ImmutableMessageParser;
import com.dexels.immutable.factory.ImmutableFactory;


@Path("/hello")
public class thing {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {
    	ImmutableMessage msg = ImmutableFactory.empty().with("aap","noot", ImmutableMessage.ValueType.STRING.name());
    	return messageParser.describe(msg);
    }
    
    @Inject ImmutableMessageParser messageParser;
}