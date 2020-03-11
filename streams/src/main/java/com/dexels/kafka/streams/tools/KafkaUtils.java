package com.dexels.kafka.streams.tools;

import com.dexels.kafka.streams.api.CoreOperators;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class KafkaUtils {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaUtils.class);

	private KafkaUtils() {
		// no instances
	}
    private static Future<Void> ensureExist(AdminClient adminClient,Collection<String> topics, int topicPartitionCount, int topicReplicationCount) {
			if(!topicsAllExist(adminClient, topics)) {
				return createTopics(adminClient, topics, topicPartitionCount, topicReplicationCount);
			}
			return KafkaFuture.completedFuture(null);
    }

    private static Future<Void> ensureExists(AdminClient adminClient,String topicName, int topicPartitionCount, int topicReplicationCount) {
			if(!topicExists(adminClient, topicName)) {
				return createTopic(adminClient, topicName, topicPartitionCount, topicReplicationCount);
			}
			return KafkaFuture.completedFuture(null);
    }
    public static void ensureExistsSync(Optional<AdminClient> adminClient,String topicName, Optional<Integer> partitionCount) {
    	ensureExistsSync(adminClient, topicName, partitionCount.orElse(CoreOperators.topicPartitionCount()), CoreOperators.topicReplicationCount());
    }
    
    public static void ensureExistSync(Optional<AdminClient> adminClient,Collection<String> topics, int topicPartitionCount, int topicReplicationCount) {
    	if(!adminClient.isPresent()) {
    		return;
    	}
    	Future<Void> ensured = ensureExist(adminClient.get(), topics, topicPartitionCount, topicReplicationCount);
    	try {
			ensured.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.warn("Issue creating topics: {}. Ignoring and continuing",topics,e);
		} catch(TopicExistsException e) {
			// ignore
		}
    }
    public static void ensureExistsSync(Optional<AdminClient> adminClient,String topicName, int topicPartitionCount, int topicReplicationCount) {
    	if(!adminClient.isPresent()) {
    		return;
    	}
    	Future<Void> ensured = ensureExists(adminClient.get(), topicName, topicPartitionCount, topicReplicationCount);
    	try {
			ensured.get();
		} catch ( ExecutionException e) {
			logger.warn("Issue creating topic: {}. Ignoring and continuing",topicName,e);
		} catch (InterruptedException e) {
			logger.warn("Issue creating topic: {}. Ignoring and continuing",topicName);
		} catch(TopicExistsException e) {
			// ignore
			
		}
    }

    private static Future<Void> createTopic(AdminClient adminClient,String topicName, int noOfPartitions, int noOfReplication) {
		CreateTopicsResult cr = adminClient.createTopics(Arrays.asList(topicName).stream().map(name->new NewTopic(name, noOfPartitions, (short)noOfReplication)).collect(Collectors.toList()));
		return cr.all();

	}
    
    private static Future<Void> createTopics(AdminClient adminClient,Collection<String> topics, int noOfPartitions, int noOfReplication) {
		CreateTopicsResult cr = adminClient.createTopics(topics.stream().map(name->new NewTopic(name, noOfPartitions, (short)noOfReplication)).collect(Collectors.toList()));
		return cr.all();

	}
    
	public static Future<Void> deleteTopic(AdminClient adminClient,String topicName) {
		DeleteTopicsResult cr = adminClient.deleteTopics(Arrays.asList(topicName));
		return cr.all();

	}
	
	public static boolean topicExists(AdminClient adminClient, String topicName) {
		try {
			Set<String> topics = adminClient.listTopics().names().get();
			return topics.contains(topicName);
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Error querying existence of topic: "+topicName, e);
		}

		return false;
	}
	
	public static boolean topicsAllExist(AdminClient adminClient, Collection<String> topics) {
		try {
			Set<String> currentTopics = adminClient.listTopics().names().get();
			return currentTopics.containsAll(topics);
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Error querying existence of topics: "+topics.stream().collect(Collectors.joining(",")), e);
		}

		return false;
	}

}
