package com.dexels.kafka.webapi;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.navajo.repository.api.RepositoryInstance;
import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.reactivex.Completable;
import io.reactivex.Flowable;

@Path("/gurgle/{tenant}/{deployment}/{generation}")
public class Aap {


	@Inject
	PersistentSubscriber persistentSubscriber;

	@Inject
	PersistentPublisher publisher;

	@Inject
	RepositoryInstance repositoryInstance;

	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	
	private final static Logger logger = LoggerFactory.getLogger(KafkaServlet.class);

	
	@PostConstruct
	public void activate() {
		System.err.println("Aap online");
	}

	
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response doGet(@PathParam("tenant") String tenant, @PathParam("deployment") String deployment, @DefaultValue("") @PathParam("generation") String genRaw) throws JsonProcessingException {
		Optional<String> generation = "".equals(genRaw)?Optional.empty(): Optional.ofNullable(genRaw);
		if(tenant==null) {
			return Response.status(400,"Expected path /<tenant>/<deployment>/<generation>").build();
		}
		JsonNode root;
		if (generation.isPresent()) {
			ArrayNode groupNode = objectMapper.createArrayNode();
			ArrayNode topicNode = objectMapper.createArrayNode();
			topics(tenant, deployment, generation.get()).blockingForEach(e->topicNode.add(e));
			Set<String> grps = groups(tenant, deployment, generation.get());
			grps.forEach(e->groupNode.add(e));
			ObjectNode rr = objectMapper.createObjectNode();
			rr.set("groups", groupNode);
			rr.set("topics", topicNode);
			root = rr;
			
		} else {
			ArrayNode rr = objectMapper.createArrayNode();
			generations(tenant, deployment).blockingForEach(e->{
				rr.add(e);
			});
			root = rr;

		}
		Response.ResponseBuilder rb = Response.ok(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root));
		return rb.build();
//		try {
//
//			return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(root); //writeValue(resp.getWriter(), root);
//		} catch (JsonProcessingException e1) {
//			throw new RuntimeException("Error forming json", e1);
//		}
//		return Response.status(400,"Expected path /<tenant>/<deployment>/<generation>").build();

	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public String doDelete(@PathParam("tenant") String tenant, @PathParam("deployment") String deployment, 
				@PathParam("generation") String genRaw,@QueryParam("real") String real) throws ServletException, IOException {
//		resp.addHeader("Access-Control-Allow-Origin", "*");
			Optional<String> generation = "".equals(genRaw)?Optional.empty(): Optional.ofNullable(genRaw);
			System.err.println("Delete!");
			Writer writer = new StringWriter();
			if(generation.isPresent()) {
				if(real==null) {
					Set<String> grps = groups(tenant, deployment, generation.get());
					writer.write("Would have deleted: \n");
					for (String e : grps) {
						writer.write("group: "+e+"\n");
					}
					topics(tenant, deployment, generation.get()).blockingForEach(e->{
						writer.write("topic: "+e+"\n");
					});
				} else {
					Completable c = KafkaTools.deleteGroups(publisher, tenant, deployment, generation.get(), writer);
					Completable ctopics = KafkaTools.deleteGenerationTopics(publisher, writer, tenant, deployment, generation.get());
					c.andThen(ctopics).blockingAwait(1,TimeUnit.HOURS);
				}
			} else {
				if(real==null) {
					Flowable<String> generationalTopics = generations(tenant, deployment)
						.flatMap(gen->topics(tenant, deployment, gen))
						.concatWith(nonGentopics(tenant, deployment));
					generationalTopics.blockingForEach(e->writer.write(e+"\n"));
					
				} else {
					generations(tenant, deployment)
						.flatMapCompletable(gen->KafkaTools.deleteGenerationTopics(publisher, writer, tenant, deployment, gen))
						.concatWith(deleteDeploymentTenant(writer,tenant,deployment))
						.blockingAwait();
				}
			}
			
			return writer.toString();
	}

	private Completable deleteDeploymentTenant(Writer writer, String tenant, String deployment) throws ServletException, IOException {
		logger.info("Deleting tenant: {} deployment: {}",tenant,deployment);
		if(tenant==null) {
			throw new ServletException("No tenant supplied!");
		}
		if(deployment==null) {
			throw new ServletException("No deployment supplied!");
		}
		TopicStructure struct = KafkaTools.getTopics(publisher).blockingGet();

		Set<String> topics = struct.getTenant(tenant).getDeployment(deployment).getNonGenerationalTopics();
		writer.write("Deleting non generational topics: "+topics);
		return Flowable.fromIterable(topics)
			.doOnNext(e->writer.write("Deleting: "+e+"\n"))
			.buffer(50)
			.flatMapCompletable(e->this.publisher.deleteCompletable(e))
			.doOnComplete(()->logger.info("done!"));
	}
	
	private Set<String> groups(String tenant, String deployment, String generation) {
		GroupStructure groups = KafkaTools.getGroupStructure(publisher).blockingGet();
		GroupStructure.Generation gr = groups.getTenant(tenant).getDeployment(deployment).getGeneration(generation);
		Set<String> grps = gr.getGroups();
		return grps;
	}
	

	private Flowable<String> topics(String tenant, String deployment, String generation) {
		return KafkaTools.getTopics(publisher).toFlowable()
				.flatMap(struct->Flowable.fromIterable(struct.getTenant(tenant).getDeployment(deployment).getGeneration(generation).getTopics()));
	}

	private Flowable<String> nonGentopics(String tenant, String deployment) {
		return KafkaTools.getTopics(publisher)
			.toFlowable()
			.flatMap(struct->Flowable.fromIterable(struct.getTenant(tenant).getDeployment(deployment).getNonGenerationalTopics()));
	}

	private Flowable<String> generations(String tenant, String deployment) {
		return KafkaTools.getTopics(publisher)
				.map(struct->struct.getTenant(tenant).getDeployment(deployment).getGenerations())
				.map(e->Flowable.fromIterable(e))
				.toFlowable()
				.flatMap(e->e);
				
	}
}
