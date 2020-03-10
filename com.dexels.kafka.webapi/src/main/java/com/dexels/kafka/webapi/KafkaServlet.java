package com.dexels.kafka.webapi;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.reactivex.Completable;
import io.reactivex.Flowable;

public class KafkaServlet {

	private static final long serialVersionUID = 8563181635935834994L;

	@Inject
	PersistentSubscriber persistentSubscriber;

	@Inject
	PersistentPublisher publisher;

	
	private static final ObjectMapper objectMapper = new ObjectMapper();

	
	private final static Logger logger = LoggerFactory.getLogger(KafkaServlet.class);

	private String deployment;
	private String repositoryPath;


	public KafkaServlet(String deployment, String repositoryPath) {
		this.deployment = deployment;
		this.repositoryPath = repositoryPath;
	}

	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.addHeader("Access-Control-Allow-Origin", "*");
		try(final PrintWriter writer = resp.getWriter()) {
			String path = req.getPathInfo();
			String real = req.getParameter("real");
			String[] parts = path.split("/");
			if(parts.length==4) {
				String tenant = parts[1];
				String deployment = parts[2];
				String generation = parts[3];
				if(real==null) {
					Set<String> grps = groups(tenant, deployment, generation);
					writer.write("Would have deleted: \n");
					for (String e : grps) {
						writer.write("group: "+e+"\n");
					}
					topics(tenant, deployment, generation).blockingForEach(e->{
						writer.write("topic: "+e+"\n");
					});
				} else {
					Completable c = KafkaTools.deleteGroups(publisher, tenant, deployment, generation, resp.getWriter());
					Completable ctopics = KafkaTools.deleteGenerationTopics(publisher, writer, tenant, deployment, generation);
					c.andThen(ctopics).blockingAwait(1,TimeUnit.HOURS);
				}
			} else if(parts.length == 3) {
				String tenant = parts[1];
				String deployment = parts[2];
				if(real==null) {
					Flowable<String> generationalTopics = generations(tenant, deployment)
						.flatMap(generation->topics(tenant, deployment, generation))
						.concatWith(nonGentopics(tenant, deployment));
					generationalTopics.blockingForEach(e->writer.write(e+"\n"));
					
				} else {
					generations(tenant, deployment)
						.flatMapCompletable(generation->KafkaTools.deleteGenerationTopics(publisher, writer, tenant, deployment, generation))
						.concatWith(deleteDeploymentTenant(resp,tenant,deployment))
						.blockingAwait();
				}
			} else if(parts.length== 1 && "shutdown".equals(parts[0])) {
				shutdownStreamInstance(writer);
			}
			
		}
	}

	private void shutdownStreamInstance(Writer writer) throws IOException, ServletException {
		String deployment = this.deployment;
		String generation = System.getenv("GENERATION");
		String tenants = System.getenv("TENANT_MASTER");
		String[] tenantArray = tenants.split(",");
		
		for (String tenant : tenantArray) {
			Completable c = KafkaTools.deleteGroups(publisher, tenant, deployment, generation, writer);
			Completable ctopics = KafkaTools.deleteGenerationTopics(publisher, writer, tenant, deployment, generation);
			c.andThen(ctopics).blockingAwait(1,TimeUnit.HOURS);
		}
		// TODO Auto-generated method stub
		
	}

	private void shutdownTenantGeneration(String tenant, String deployment, String generation) {
		
	}
	

	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.addHeader("Access-Control-Allow-Origin", "*");
		resp.setContentType("application/json");
		String path = req.getPathInfo();
		if(path==null) {
			resp.sendError(400,"Expected path /<tenant>/<deployment>/<generation>");
			return;
		}
		JsonNode root;
		String[] parts = path.split("/");
		if(parts.length==4) {
//			0 == ""
			String tenant = parts[1];
			String deployment = parts[2];
			String generation = parts[3];
			ArrayNode groupNode = objectMapper.createArrayNode();
			ArrayNode topicNode = objectMapper.createArrayNode();
			topics(tenant, deployment, generation).blockingForEach(e->topicNode.add(e));
			Set<String> grps = groups(tenant, deployment, generation);
			grps.forEach(e->groupNode.add(e));
			ObjectNode rr = objectMapper.createObjectNode();
			rr.set("groups", groupNode);
			rr.set("topics", topicNode);
			root = rr;

		} else if(parts.length==3) {
			String tenant = parts[1];
			String deployment = parts[2];
			ArrayNode rr = objectMapper.createArrayNode();
			generations(tenant, deployment).blockingForEach(e->{
				rr.add(e);
			});
			root = rr;
		} else {
			resp.sendError(400,"Expected path /<tenant>/<deployment>/<generation>");
			return;
		}
		try {
			
			objectMapper.writerWithDefaultPrettyPrinter().writeValue(resp.getWriter(), root);
		} catch (JsonGenerationException e1) {
			throw new ServletException("Error forming json", e1);
		} catch (JsonMappingException e1) {
			throw new ServletException("Error forming json", e1);
		}
	}

	private Set<String> groups(String tenant, String deployment, String generation) {
		GroupStructure groups = KafkaTools.getGroupStructure(publisher).blockingGet();
		GroupStructure.Generation gr = groups.getTenant(tenant).getDeployment(deployment).getGeneration(generation);
		Set<String> grps = gr.getGroups();
		return grps;
	}
	
	private Flowable<String> nonGenerationalTopics(String tenant, String deployment) {
		return KafkaTools.getTopics(publisher).toFlowable()
				.flatMap(struct->Flowable.fromIterable(struct.getTenant(tenant).getDeployment(deployment).getNonGenerationalTopics()));
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

	private Completable deleteDeploymentTenant(HttpServletResponse resp, String tenant, String deployment) throws ServletException, IOException {
		logger.info("Deleting tenant: {} deployment: {}",tenant,deployment);
		if(tenant==null) {
			throw new ServletException("No tenant supplied!");
		}
		if(deployment==null) {
			throw new ServletException("No deployment supplied!");
		}
		TopicStructure struct = KafkaTools.getTopics(publisher).blockingGet();

		Set<String> topics = struct.getTenant(tenant).getDeployment(deployment).getNonGenerationalTopics();
		resp.getWriter().write("Deleting non generational topics: "+topics);
		return Flowable.fromIterable(topics)
			.doOnNext(e->resp.getWriter().write("Deleting: "+e+"\n"))
			.buffer(50)
			.flatMapCompletable(e->this.publisher.deleteCompletable(e))
			.doOnComplete(()->logger.info("done!"));
	}
}
