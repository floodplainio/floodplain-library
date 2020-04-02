package com.dexels.kafka.api;

import java.util.Optional;

public class KafkaTopicPublisherConfiguration {

    Optional<Integer> retries;
    Short replicationFactor;
    Integer partitions;

    Optional<String> compression;
    String bootstrapServers;

    public Optional<Integer> retries() {
        return retries;
    }

    public Short replicationFactor() {
        return replicationFactor;
    }

    public Integer partitions() {
        return partitions;
    }

    public Optional<String> compression() {
        if (partitions == null) {
            return Optional.empty();
        }
        return compression;
    }

    public KafkaTopicPublisherConfiguration(String bootstrapServers, Optional<Integer> retries, short replicationFactor, int partitions, Optional<String> compression) {
        this.bootstrapServers = bootstrapServers;
        this.retries = retries;
        this.replicationFactor = replicationFactor;
        this.partitions = partitions;
        this.compression = compression;
    }

    public String bootstrapServers() {
        return this.bootstrapServers;
    }
}
