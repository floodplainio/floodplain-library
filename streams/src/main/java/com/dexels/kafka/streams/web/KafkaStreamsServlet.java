package com.dexels.kafka.streams.web;

import com.dexels.kafka.streams.base.StreamRuntime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.osgi.service.component.annotations.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component(name = "dexels.kafkastreams.servlet", service = Servlet.class, immediate = true, property = {"alias=/streams", "servlet-name=streams"}, configurationPolicy = ConfigurationPolicy.OPTIONAL)
public class KafkaStreamsServlet extends HttpServlet {
    private static final long serialVersionUID = -567576664126642365L;


    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsServlet.class);

    private StreamRuntime runtime;

    @Reference(policy = ReferencePolicy.DYNAMIC, unbind = "clearStreamRuntime", cardinality = ReferenceCardinality.OPTIONAL)
    public void setStreamRuntime(StreamRuntime runtime) {
        this.runtime = runtime;
    }

    public void clearStreamRuntime(StreamRuntime runtime) {
        this.runtime = null;
    }


    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
        if (runtime == null) {
            response.sendError(500, "Missing StreamRuntime");
            return;
        }
        if ("/status".equals(req.getPathInfo())) {
            boolean ok = true;
            String output = "";
            for (String name : runtime.getStreams().keySet()) {
                for (KafkaStreams kafkaStream : runtime.getStreams().get(name).getStreams()) {
                    State state = kafkaStream.state();
                    if (!state.isRunning()) {
                        String streamStatus = kafkaStream.toString();
                        String splitPart = "StreamsThread appId:";
                        final int indexOf = streamStatus.indexOf(splitPart);
                        if (indexOf == -1) {
                            // this won't end well
                            logger.warn("Problem parsing streamStatus >{}<", streamStatus);
                        }
                        String part1 = streamStatus.substring(indexOf);
                        String part2 = part1.substring(splitPart.length() + 1);
                        streamStatus = part2.substring(0, part2.indexOf("\n"));

                        ok = false;
                        output += "Stream " + streamStatus + " Is in state: " + state + "\n";
                    }
                }
            }
            if (!ok) {
                response.sendError(500, output);
            } else {
                response.getWriter().write("OK");
            }
            return;
        }


        for (String name : runtime.getStreams().keySet()) {
            response.getWriter().write("-------------------------------- " + name + "\n");
            for (KafkaStreams kafkaStream : runtime.getStreams().get(name).getStreams()) {
                String debug = req.getParameter("debug");
                response.getWriter().write("State " + kafkaStream.state() + "\n");
                String streamStatus = kafkaStream.toString();
                response.getWriter().write(streamStatus);

                response.getWriter().write("\n ");

            }

            response.getWriter().write("------------------------------------------------------------------------\n\n");
        }
    }


}
