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

import java.util.Optional;

public class TopologyContext {

    public static final String DEFAULT_TENANT = "DEFAULT";
    public final Optional<String> tenant;
    public final String deployment;
    public final String instance;
    public final String generation;
//	public final String brokers;


    public TopologyContext(Optional<String> tenant, String deployment, String instance, String generation) {
        this.tenant = tenant;
        this.deployment = deployment;
        this.instance = instance;
        this.generation = generation;
//		this.brokers = brokers;
    }

    public TopologyContext withInstance(String newInstance) {
        return new TopologyContext(tenant, deployment, newInstance, generation);
    }

    public String applicationId() {
        return tenant.orElse(DEFAULT_TENANT) + "-" + deployment + "-" + generation + "-" + instance;
    }

    public String qualifiedName(String name, int currentTransformer, int currentPipe) {
        return CoreOperators.topicName("@" + name + "_" + currentPipe + "_" + currentTransformer, this);
    }

}
