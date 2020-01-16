package io.floodplain;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;

import org.jboss.resteasy.annotations.Stream;

@Path("/reactive")
public class TestReactive {
	@GET @Stream
    @Produces(MediaType.TEXT_PLAIN)
    @Consumes(MediaType.TEXT_PLAIN)
    public Flowable<String> hello(Flowable<String> input) {
    	return Observable.interval(1, TimeUnit.MILLISECONDS)
    		.take(1000)
    		.map(i->"xxxxxxxxxxxxxxxxxxxxxxxx xxxxxxx yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"+i)
    		.toFlowable(BackpressureStrategy.BUFFER);
    }
	
	
}