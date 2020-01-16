package io.floodplain;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;


@Path("/hello")
public class HelloReactive {

	   @GET
	   @Path("/rxget/{customerName}")
	   @Produces("application/json")
	   public void getCollectedDataList(@Suspended AsyncResponse async, @PathParam("customerName") String customerName) {

	      List<String> ids = null;

//	      synchronized (store) {
//	         ids = store.get(customerName);
//	         store.put(customerName, new ArrayList<String>(Arrays.asList(customerName)));
//	      }

	      async.resume(new GenericEntity<List<String>>(ids) {});
	   }
}