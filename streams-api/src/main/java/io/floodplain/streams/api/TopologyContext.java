/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.floodplain.streams.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class TopologyContext {

    private static final String DEFAULT_TENANT = "DEFAULT";
    private final Optional<String> tenant;
    private final String deployment;
    private final String instance;
    private final String generation;

    private static final Logger logger = LoggerFactory.getLogger(TopologyContext.class);

    public static TopologyContext context(String tenant, String deployment, String instance, String generation) {
        return new TopologyContext(Optional.of(tenant),deployment,instance,generation);
    }

    public static TopologyContext context(String deployment, String instance, String generation) {
        return new TopologyContext(Optional.empty(),deployment,instance,generation);
    }



    private TopologyContext(Optional<String> tenant, String deployment, String instance, String generation) {
        this.tenant = tenant;
        this.deployment = deployment;
        this.instance = instance;
        this.generation = generation;
    }

    public String applicationId() {
        return tenant.orElse(DEFAULT_TENANT) + "-" + deployment + "-" + generation + "-" + instance;
    }

    public String qualifiedName(String name, int currentTransformer, int currentPipe) {
        return topicName("@" + name + "_" + currentPipe + "_" + currentTransformer);
    }


    public String topicName(String topicName) {
        if (topicName.contains("-generation-")) {
            logger.warn("Warning: Re-resolving topic: {}", topicName);
            Thread.dumpStack();
        }
        if(topicName.indexOf("-")!=-1) {
            throw new RuntimeException("Can't use topic names containing a '-'");
        }
        String topic = topicNameForReal(topicName);
        if (topic.indexOf('@') != -1) {
            throw new UnsupportedOperationException("Bad topic: " + topic + " from instance: " + instance + " tenant: " + tenant + " deployment: " + deployment + " generation: " + generation);
        }
        return topic;
    }

    public String generationalGroup(String name) {
        if (name.startsWith("@")) {
            String[] withInstance = name.split(":");
            if (tenant.isPresent()) {
                if (withInstance.length > 1) {
                    return tenant.get() + "-" + deployment + "-" + generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                } else {
                    return tenant.get() + "-" + deployment + "-" + generation + "-" + instance + "-" + name.substring(1);
                }
            } else {
                if (withInstance.length > 1) {
                    return deployment + "-" + generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                } else {
                    return deployment + "-" + generation + "-" + instance + "-" + name.substring(1);
                }
            }
        }
        return tenant.map(s -> s + "-" + deployment + "-" + generation + "-" + instance + "-" + name).orElseGet(() -> deployment + "-" + generation + "-" + instance + "-" + name);
    }


    private String topicNameForReal(String name) {
        if (name == null) {
            throw new NullPointerException("Can not create topic name when name is null. tenant: " + tenant.orElse("<no tenant>") + " deployment: " + deployment + " generation: " + generation);
        }
        if (name.startsWith("@")) {
            StringBuffer sb = new StringBuffer();
            tenant.ifPresent(s -> sb.append(s + "-"));
            sb.append(deployment + "-" + generation + "-" + instance + "-" + name.substring(1));
            return sb.toString();
        } else {
aaa
        }
        return tenant.map(s -> s + "-" + deployment + "-" + name).orElseGet(() -> deployment + "-" + name);
    }
}
