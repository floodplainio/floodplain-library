package io.floodplain.streams.api;

public class Topic {

    private final String topicDefinition;

    private Topic(String topicDefinition) {
        this.topicDefinition = topicDefinition;
    }

    public static Topic from(String topicDefinition) {
        return new Topic(topicDefinition);
    }

    public String qualifiedString(TopologyContext topologyContext) {
        return topologyContext.topicName(topicDefinition);
    }

    public String prefixedString(String prefix, TopologyContext topologyContext) {
        return prefix+"_"+topologyContext.topicName(topicDefinition);
    }

    public String toString() {
        return "AHAHAHAHAA";
    }
    public String unqualified() {
        return topicDefinition;
    }

}
