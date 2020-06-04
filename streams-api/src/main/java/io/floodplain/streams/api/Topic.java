package io.floodplain.streams.api;

import java.util.Objects;

public class Topic {

    private final String topicDefinition;
    private final String qualifiedDefinition;

//    private static final TopologyContext mockContext = TopologyContext.context("instance","gen");
    private Topic(String topicDefinition, String qualifiedDefinition) {
        this.topicDefinition = topicDefinition;
        this.qualifiedDefinition = qualifiedDefinition;
    }

    public static Topic from(String topicDefinition) {
        return new Topic(topicDefinition,null);
    }

    public static Topic fromQualified(String qualifiedDefinition) {
        return new Topic(null,qualifiedDefinition);
    }

    public String qualifiedString(TopologyContext topologyContext) {
        if(qualifiedDefinition!=null) {
            return qualifiedDefinition;
        }
        return topologyContext.topicName(topicDefinition);
    }

    public String prefixedString(String prefix, TopologyContext topologyContext) {
        return prefix+"_"+qualifiedString(topologyContext);
    }

    public String toString() {
        if(topicDefinition!=null) {
            return topicDefinition;
        } else {
            return unqualify(qualifiedDefinition);
        }
    }

    private String unqualify(String qualified) {
        String[] parts = qualified.split("-");
        if(parts.length < 2) {
            throw new RuntimeException("Can not unqualify topic: "+qualified+" as there are not enough parts");
        }
        if(parts.length >= 3) {
            return "@"+parts[parts.length-1];

        }
        return parts[parts.length-1];
    }
    public String unqualified() {
        return topicDefinition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic)) return false;
        Topic topic = (Topic) o;
        return this.toString().equals(topic.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }
}
