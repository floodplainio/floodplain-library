package com.dexels.kafka.webapi;

import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.TopicPublisher;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

@Component(name = "dexels.kafka.group.servlet", property = {"servlet-name=dexels.kafka.group.servlet", "alias=/group", "asyncSupported.Boolean=true"}, immediate = true, service = {Servlet.class})
public class GroupServlet extends HttpServlet implements Servlet {

    private static final long serialVersionUID = 8563181635935834994L;
    private PersistentPublisher publisher;

    private static final ObjectMapper objectMapper = new ObjectMapper();


    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "clearTopicPublisher")
    public void setTopicPublisher(PersistentPublisher publisher) {
        this.publisher = publisher;
    }


    public void clearTopicPublisher(TopicPublisher publisher) {
        this.publisher = null;
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        boolean doList = req.getParameter("list") != null;
        boolean all = req.getParameter("all") != null;
        boolean pretty = req.getParameter("pretty") != null;
        boolean desc = req.getParameter("desc") != null;
        String group = req.getParameter("group");
        String command = req.getParameter("cmd");
        Optional<String> tenant = Optional.ofNullable(req.getParameter("tenant"));
        Optional<String> deployment = Optional.ofNullable(req.getParameter("deployment"));
        Optional<String> generation = Optional.ofNullable(req.getParameter("generation"));
        if (all) {
            groupDescription(group, resp, pretty, tenant, deployment, generation);
            return;
        }
        if (doList) {
            groupList(req, resp, pretty);
            return;
        }
        if (command != null) {
            switch (command) {
                case "delete":
                    if (req.getParameter("real") != null) {
                        KafkaTools.deleteGroups(publisher, tenant.get(), deployment.get(), generation.get(), resp.getWriter());
                    } else {
                        KafkaTools.deleteDryRun(publisher, tenant.get(), deployment.get(), generation.get(), resp.getWriter());
                    }
                    return;

                default:
                    break;
            }
        }
        if (group != null) {

            // combine these:
            if (desc) {
                describeGroup(group, req, resp, pretty);
            }
            groupOffsets(group, req, resp, pretty);
        }
//		downloadTopic(req, resp);
    }


    @Override
    public void init(ServletConfig config) throws ServletException {
        System.err.println("Initialize...");
        super.init(config);
        final Enumeration<String> names = config.getServletContext().getAttributeNames();
        while (names.hasMoreElements()) {
            String name = names.nextElement();
            System.err.println(" > " + name);
        }
    }

    private void describeGroup(String group, HttpServletRequest req, HttpServletResponse resp, boolean pretty) throws IOException, ServletException {
        ObjectNode an = objectMapper.createObjectNode();

        publisher.describeConsumerGroups(Arrays.asList(new String[]{group}))
                .firstOrError()
                .blockingGet()
                .entrySet()
                .stream()
                .forEach(e -> {
                    an.put(e.getKey(), e.getValue());
                });


        ObjectWriter w = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
        w.writeValue(resp.getWriter(), an);

    }

    private void groupOffsets(String group, HttpServletRequest req, HttpServletResponse resp, boolean pretty) throws IOException, ServletException {
        Map<String, Long> p = publisher.consumerGroupOffsets(group)
                .blockingGet();
        ObjectNode an = objectMapper.createObjectNode();
        ObjectNode topics = objectMapper.createObjectNode();
        p.entrySet().forEach(e -> {
            topics.put(e.getKey(), e.getValue());
        });
        an.set(group, topics);
        try {
            ObjectWriter w = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
            w.writeValue(resp.getWriter(), an);
        } catch (JsonGenerationException e1) {
            throw new ServletException("Error forming json", e1);
        } catch (JsonMappingException e1) {
            throw new ServletException("Error forming json", e1);
        }
    }


    private void groupList(HttpServletRequest req, HttpServletResponse resp, boolean pretty) throws IOException, ServletException {
//		AsyncContext ac = req.startAsync();
//		ac.setTimeout(1000000);
//		ResponseSubscriber responseSubscriber = new ResponseSubscriber(ac);

        ArrayNode an = objectMapper.createArrayNode();
        List<String> list = publisher.listConsumerGroups()
                .reduce(new ArrayList<String>(), (l, e) -> {
                    l.add(e);
                    return l;
                })
                .blockingGet();

        ;
        list.stream()
                .filter(e -> {
                    return !e.startsWith("NavajoLink") &&
                            !e.startsWith("AAAResetter") &&
                            !e.startsWith("console-consumer");
                }).forEach(item -> an.add(item));
//		for (String item : list) {
//			an.add(item);
//		}
//		TopicStructure struct = publisher.streamTopics()
//				.reduce(new TopicStructure(), (stru,e)->stru.consumeTopic(e))
//				.blockingGet();

        try {
            ObjectWriter w = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
            w.writeValue(resp.getWriter(), an);
        } catch (JsonGenerationException e1) {
            throw new ServletException("Error forming json", e1);
        } catch (JsonMappingException e1) {
            throw new ServletException("Error forming json", e1);
        }

    }

    private void groupDescription(String group, HttpServletResponse resp, boolean pretty, Optional<String> tenant, Optional<String> deployment, Optional<String> generation) throws IOException, ServletException {

        try {
            GroupStructure gr = KafkaTools.getGroupStructure(publisher)
                    .blockingGet();

            try {
                if (!tenant.isPresent()) {
                    ObjectWriter writer = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
                    writer.writeValue(resp.getWriter(), gr);
                } else {
                    GroupStructure.Tenant t = gr.getTenant(tenant.get());
                    if (t == null) {
                        throw new ServletException("Error getting tenant: " + tenant.get() + ". It is missing.");
                    }
                    if (!deployment.isPresent()) {
                        ObjectWriter writer = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
                        writer.writeValue(resp.getWriter(), t);
                    } else {
                        GroupStructure.Deployment d = t.getDeployment(deployment.get());
                        if (d == null) {
                            throw new ServletException("Error getting deployment: " + deployment.get() + ", from tenant: " + tenant.get() + " It is missing.");
                        }
                        if (!generation.isPresent()) {
                            ObjectWriter writer = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
                            writer.writeValue(resp.getWriter(), d);
                        } else {
                            GroupStructure.Generation g = d.getGeneration(generation.get());
                            if (g == null) {
                                throw new ServletException("Error getting generation: " + generation.get() + ", from tenant: " + tenant.get() + " and deployment: " + deployment.get() + " It is missing.");
                            }
                            ObjectWriter writer = pretty ? objectMapper.writerWithDefaultPrettyPrinter() : objectMapper.writer();
                            writer.writeValue(resp.getWriter(), g);

                        }

                    }
                }
            } catch (JsonGenerationException e1) {
                throw new ServletException("Error forming json", e1);
            } catch (JsonMappingException e1) {
                throw new ServletException("Error forming json", e1);
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

}
