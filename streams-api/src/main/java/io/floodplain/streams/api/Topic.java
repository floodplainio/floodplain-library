/**
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

import java.util.Objects;

public class Topic {

    private final String topicDefinition;
    private final String qualifiedDefinition;
    private final TopologyContext topologyContext;

    public enum FloodplainBodyFormat {
        CONNECT_JSON, FLOODPLAIN_JSON
    }

    public enum FloodplainKeyFormat {
        CONNECT_KEY_JSON, FLOODPLAIN_STRING
    }


    //    private static final TopologyContext mockContext = TopologyContext.context("instance","gen");
    private Topic(TopologyContext topologyContext, String topicDefinition, String qualifiedDefinition) {
        this.topologyContext = topologyContext;
        // only one will be used
        this.topicDefinition = topicDefinition;
        this.qualifiedDefinition = qualifiedDefinition;
        if(qualifiedDefinition!=null && qualifiedDefinition.contains("@")) {
            throw new IllegalArgumentException("Qualified definitions can not have an '@'");
        }
    }

    @Deprecated
    public static Topic from(String topicDefinition, TopologyContext topologyContext) {
        return new Topic(topologyContext,topicDefinition,null);
    }

    public static Topic fromQualified(String qualifiedDefinition, TopologyContext topologyContext) {
        return new Topic(topologyContext,null,qualifiedDefinition);
    }

    public String qualifiedString() {
        if(qualifiedDefinition!=null) {
            return qualifiedDefinition;
        }
        return topologyContext.topicName(topicDefinition);
    }

    public String prefixedString(String prefix) {
        return prefix+"_"+qualifiedString();
    }

    public String toString() {
        return qualifiedString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic)) return false;
        Topic topic = (Topic) o;
        return this.qualifiedString().equals(topic.qualifiedString());
//        return this.toString().equals(topic.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }
}
