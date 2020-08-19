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

    private static class NameQualifier implements Function<String,String> {

        private final Optional<String> tenant;
        private final String instance;
        private final String generation;

        public NameQualifier(Optional<String> tenant, String instance, String generation) {
            this.tenant = tenant;
            this.instance = instance;
            this.generation = generation;
        }
        @Override
        public String apply(String name) {
            // Dashes are problematic. Maybe hard-fail whenever we create a source / sink that contains a dash?
            // Otherwise this creates a weird 'silent error state' where simply nothing happens.
            if(name.contains("-") || name.contains(":")) {
                return name;
            }
            if(!name.startsWith("@") && name.contains("@")) {
                logger.warn("This is problematic: {}",name);
                Thread.dumpStack();
            }
            if (name.startsWith("@")) {
                String[] withInstance = name.split(":");
                if (tenant.isPresent()) {
                    if (withInstance.length > 1) {
                        return tenant.get() + "-" + generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                    } else {
                        return tenant.get() + "-" + generation + "-" + instance + "-" + name.substring(1);
                    }
                } else {
                    if (withInstance.length > 1) {
                        return generation + "-" + withInstance[0].substring(1) + "-" + withInstance[1];
                    } else {
                        return generation + "-" + instance + "-" + name.substring(1);
                    }
                }
            } else {
                return tenant.map(s -> s + "-" + instance + "-" + name).orElseGet(() -> instance + "-" + name);
            }
        }
    }

    public static TopologyContext context(Function<String,String> qualifier) {
        return new TopologyContext(qualifier);
    }
    public static TopologyContext context(Optional<String> tenant, String instance, String generation) {
        return new TopologyContext(new NameQualifier(tenant,instance,generation));
    }

    public TopologyContext(Function<String, String> qualifier) {
        this.qualifier = qualifier;
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
