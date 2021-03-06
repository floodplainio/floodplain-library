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
package io.floodplain.streams.remotejoin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Create alarm topics
 *  @author Evan Smith
 */
@SuppressWarnings("nls")
public class  CreateTopics
{
    private static final Logger logger = LoggerFactory.getLogger(CreateTopics.class);
    // Default configuration for each topic.
    private static final short REPLICATION_FACTOR = 1;
    private static final int PARTITIONS = 1;
    private static final String cleanup_policy = "cleanup.policy",
            compact_policy = "compact",
            delete_policy = "delete",
            segment_time = "segment.ms",
            time = "10000",
            dirty2clean = "min.cleanable.dirty.ratio",
            ratio = "0.01";

    /** Discover the currently active Kafka topics, and creates any that are missing.
     *
     *  @param kafka_servers The network address for the kafka_servers. Example: 'localhost:9092'.
     *  @param compact If the topics to be created should be compacted.
     *  @param topics Topics to discover and create if missing.
     */
    public static void discoverAndCreateTopics (final String kafka_servers, final boolean compact, final List<String> topics)
    {
        // Connect to Kafka server.
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafka_servers);
        final AdminClient client = AdminClient.create(props);

        final List<String> topics_to_create = discoverTopics(client, topics);
        createTopics(client, compact, topics_to_create);

        client.close();
    }

    /**
     * <p> Discover any currently active Kafka topics. Return a list of strings filled with any default topics that need to be created.
     * @param client
     * @return topics_to_create <code>List</code> of <code>Strings</code> with all the topic names that need to be created.
     *                           Returns <code>null</code> if none need to be created.
     */
    private static List<String> discoverTopics(final AdminClient client, final List<String> topics_to_discover)
    {
        final List<String> topics_to_create = new ArrayList<>();

        // Discover what topics currently exist.
        try
        {
            final ListTopicsResult res = client.listTopics();
            final KafkaFuture<Set<String>> topics = res.names();
            final Set<String> topic_names = topics.get();

            for (String topic : topics_to_discover)
            {
                if ( ! topic_names.contains(topic))
                    topics_to_create.add(topic);
            }
        }
        catch (Exception ex)
        {
            logger.warn("Unable to list topics. Automatic topic detection failed.", ex);
        }

        return topics_to_create;
    }

    /** Create a topic for each of the topics in the passed list.
     *  @param client {@link AdminClient}
     *  @param compact If the topics should be compacted.
     *  @param topics_to_create {@link List} of {@link String}s filled with the names of topics to create.
     */
    private static void createTopics(final AdminClient client, final boolean compact, final List<String> topics_to_create)
    {
        // Create the new topics locally.
        final List<NewTopic> new_topics = new ArrayList<>();
        for (String topic : topics_to_create)
        {
            logger.info("Creating topic '" + topic + "'");
            new_topics.add(createTopic(client, compact, topic));
        }
        // Create the new topics in the Kafka server.
        try
        {
            final CreateTopicsResult res = client.createTopics(new_topics);
            final KafkaFuture<Void> future = res.all();
            future.get();
        }
        catch (Exception ex)
        {
            logger.warn("Attempt to create topics failed", ex);
        }
    }

    /** Create a Kafka topic with the passed name. If compact is true then the cleanup policy is compact, delete.
     *  @param client {@link AdminClient}
     *  @param compact If the topic should be compacted.
     *  @param topic_name Name of the topic to be created.
     *  @return new_topic The newly created topic.
     */
    private static NewTopic createTopic(final AdminClient client, final boolean compact, final String topic_name)
    {
        final NewTopic new_topic = new NewTopic(topic_name, PARTITIONS, REPLICATION_FACTOR);
        final Map<String, String> configs = new HashMap<>();
        configs.put(segment_time, time);
        if (compact)
        {
            configs.put(cleanup_policy, compact_policy);
            configs.put(dirty2clean, ratio);
        }
        return new_topic.configs(configs);
    }
}