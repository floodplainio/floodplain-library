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
import java.util.function.Function;

public class TopologyContext {

    private static final Logger logger = LoggerFactory.getLogger(TopologyContext.class);
    private final Function<String, String> qualifier;
    private final Optional<String> tenant;
    private final Optional<String> deployment;
    private final String generation;

    public Optional<String> getTenant() {
        return this.tenant;
    }

    public Optional<String> getDeployment() {
        return this.deployment;
    }

    public String getGeneration() {
        return this.generation;
    }

    private static class NameQualifier implements Function<String,String> {

        private final Optional<String> tenant;
        private final Optional<String> deployment;
        private final String generation;

        public NameQualifier(Optional<String> tenant, Optional<String> deployment, String generation) {
            this.tenant = tenant;
            this.deployment = deployment;
            this.generation = generation;
        }
        @Override
        public String apply(String name) {
            // Dashes are problematic. Maybe hard-fail whenever we create a source / sink that contains a dash?
            // Otherwise this creates a weird 'silent error state' where simply nothing happens.
            // TODO removed return on containing a dash
//            if(name.contains("-") || name.contains(":")) {
            if( name.contains(":")) {
                return name;
            }
            long dashCount = name.chars().filter(ch -> ch == '-').count();
            if(dashCount > 1) {
                logger.warn("Multidash -> This is problematic: {}",name);
            }
            if(!name.startsWith("@") && name.contains("@")) {
                logger.warn("This is problematic: {}",name);
                Thread.dumpStack();
            }
            if (name.startsWith("@")) {
                String[] withInstance = name.split(":");
                if (tenant.isPresent()) {
                    if (withInstance.length > 1) {
                        return tenant.get() + "-" + deployment.map(e->e+"-").orElse("") + generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                    } else {
                        return tenant.get() + "-" + deployment.map(e->e+"-").orElse("") + generation + "-" + name.substring(1);
                    }
                } else {
                    if (withInstance.length > 1) {
                        return generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                    } else {
                        return generation  + "-" + name.substring(1);
                    }
                }
            } else {
                return tenant.map(s -> s + "-").orElse("") + deployment .map(e->e+"-").orElse("")+ name;
            }
        }
    }

    private static TopologyContext context(String generation, Function<String,String> qualifier) {
        return new TopologyContext(Optional.empty(),Optional.empty(),generation, qualifier);
    }
    public static TopologyContext context(Optional<String> tenant, Optional<String> deployment, String generation) {
        return new TopologyContext(tenant,deployment,generation,new NameQualifier(tenant,deployment,generation));
    }

    public static TopologyContext context(Optional<String> tenant, String generation) {
        return new TopologyContext(tenant,Optional.empty(),generation,new NameQualifier(tenant,Optional.empty(),generation));
    }

    public TopologyContext(Optional<String> tenant, Optional<String> deployment, String generation, Function<String, String> qualifier) {
        this.qualifier = qualifier;
        this.tenant = tenant;
        this.deployment = deployment;
        this.generation = generation;
    }

    public String applicationId() {
        return topicName("@applicationId");
    }

    public String qualifiedName(String name, int currentTransformer, int currentPipe) {
        return topicName("@" + name + "_" + currentPipe + "_" + currentTransformer);
    }


    public String topicName(String topicName) {
        return this.qualifier.apply(topicName);
    }

}
