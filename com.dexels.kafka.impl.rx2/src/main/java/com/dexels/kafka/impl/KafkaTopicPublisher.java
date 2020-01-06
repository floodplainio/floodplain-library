package com.dexels.kafka.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.api.KafkaTopicPublisherConfiguration;
import com.dexels.pubsub.rx2.api.MessagePublisher;
import com.dexels.pubsub.rx2.api.PersistentPublisher;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicPublisher;

import io.reactivex.Completable;
import io.reactivex.Flowable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.subscribers.DisposableSubscriber;


@Component(name="navajo.resource.kafkatopicpublisher", configurationPolicy=ConfigurationPolicy.REQUIRE, immediate=true)
@ApplicationScoped
public class KafkaTopicPublisher implements PersistentPublisher,TopicPublisher {

	private static final Logger logger = LoggerFactory.getLogger(KafkaTopicPublisher.class);
	private KafkaProducer<String,byte[]> producer = null;
	private AdminClient adminClient;
	private Integer partitions;
	private Short replicationFactor;
	private final Set<String> detectedTopics = new HashSet<>();
	
	@Activate
	public void activate(Map<String,Object> settings) {
		String bootstrapHosts = (String) settings.get("hosts");
		Optional<Integer> retries = Optional.ofNullable(((String)settings.get("retries"))).map(Integer::parseInt);
		int partitions = Optional.ofNullable(((String)settings.get("partitions"))).map(Integer::parseInt).orElseThrow(()->new RuntimeException("Missing 'partitions' setting for kafka topic publisher"));
		short replicationFactor = Optional.ofNullable(((String)settings.get("replicationFactor"))).map(Short::parseShort).orElseThrow(()->new RuntimeException("Missing 'replicationFactor' setting for kafka topic publisher"));
		Optional<String> compression = Optional.ofNullable((String) settings.get("compression"));

		KafkaTopicPublisherConfiguration publisherConfig = new KafkaTopicPublisherConfiguration(bootstrapHosts, retries, replicationFactor, partitions,compression);
		
		activate(publisherConfig);
		
	}

	@Inject
	public void activate(KafkaTopicPublisherConfiguration publisherConfig) {
		Properties props = new Properties();
		props.put("bootstrap.servers", publisherConfig.bootstrapServers());
		props.put("acks", "all");
		props.put("batch.size", 16384);
		props.put("request.timeout.ms", 90000);
		if(publisherConfig.compression().isPresent()) {
			props.put("compression.type", publisherConfig.compression());
		}
		props.put("linger.ms", 50);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", StringSerializer.class.getCanonicalName());
		props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		props.put("retries", 100);
		if(publisherConfig.retries().isPresent()) {
			props.put("retries", publisherConfig.retries().get());
		}

		this.partitions = publisherConfig.partitions();

		this.replicationFactor = publisherConfig.replicationFactor();

		
		String clientId = UUID.randomUUID().toString();
		if(clientId!=null) {
			props.put("client.id", clientId);
		}
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		Thread.currentThread().setContextClassLoader(KafkaClient.class.getClassLoader());
		try {
			producer = new KafkaProducer<>(props);
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
		this.adminClient = createAdminClient(publisherConfig);
		detectedTopics.addAll(listTopics());
	}
	
	private Set<String> listTopics() {
		try {
			final Set<String> detected = adminClient.listTopics().names().get();
			logger.info("Kafka topic publisher detected: {} topics.",detected.size());
			return detected;
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			logger.error("Error listing kafka topics for warm cache.", e);
		} catch (ExecutionException e) {
			logger.error("Error listing topics: ", e);
		}
		return Collections.emptySet();
	}


	@Override
	public Flowable<String> streamTopics() {
			final KafkaFuture<Set<String>> detected = adminClient.listTopics(new ListTopicsOptions().listInternal(false)).names();
			return Single.fromFuture(detected).toFlowable().flatMap(e->Flowable.fromIterable(e));
	}

	private AdminClient createAdminClient(KafkaTopicPublisherConfiguration publisherConfiguration) {
		Map<String,Object> adminSettings = new HashMap<>();
		adminSettings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, publisherConfiguration.bootstrapServers());
		return AdminClient.create(adminSettings);
	}
	
	@Deactivate
	public void deactivate() {
		if(this.producer!=null) {
			this.producer.close();
		}
		this.producer = null;
		this.replicationFactor = null;
		this.partitions = null;
	}
	
	@Override
	public void flush() {
		producer.flush();
	}

	
	@Override
	@Deprecated
	/**
	 * @deprecated
	 */
	public MessagePublisher publisherForTopic(final String topic) {
		if(this.producer==null ) {
			throw new RuntimeException("Can not publish to topic: "+topic+", my producer is down or unconfigured");
		}
		return new MessagePublisher() {
			@Override
			public void publish(String key, byte[] value, Consumer<Object> onSuccess, Consumer<Throwable> onFail) {
				KafkaTopicPublisher.this.publish(topic, key, value, onSuccess, onFail);
			}
			
			@Override
			public void publish(String key, byte[] value) throws IOException {
				KafkaTopicPublisher.this.publish(topic, key, value);
			}
			
			@Override
			public void flush() {
				producer.flush();
			}


			@Override
			public void create() {
				KafkaTopicPublisher.this.create(topic,Optional.empty(),Optional.empty());
			}

			@Override
			public void delete() {
				KafkaTopicPublisher.this.delete(topic);
			}

			@Override
			public Subscriber<PubSubMessage> backpressurePublisher() {
				return KafkaTopicPublisher.this.backpressurePublisher(Optional.of(topic),500);
			}
		};
	}

	@Override
	public void publish(String topic, String key, byte[] value) {
		producer.send(new ProducerRecord<String, byte[]>(topic,key, value));
	}

	@Override
	public void publish(String topic, String key, byte[] value, Consumer<Object> onSuccess,
			Consumer<Throwable> onFail) {
		producer.send(new ProducerRecord<String, byte[]>(topic,key, value),(record,exception)->{
			if(exception!=null) {
				if(onFail!=null) {
					try {
						onFail.accept(exception);
					} catch (Exception e) {
						logger.error("Error producing: ", e);
					}
				}
			} else {
				if(onSuccess!=null) {
					try {
						onSuccess.accept(record);
					} catch (Exception e) {
						logger.error("Error: ", e);
					}
				}
			}
		});		
	}
	
	public void create(String topic) {
		create(topic,Optional.empty(),Optional.empty());
	}
	
	@Override
	public void create(String topic,Optional<Integer> replicationFactor, Optional<Integer> partitionCount) {
		if(detectedTopics.contains(topic)) {
			logger.info("Topic: {} already exists, ignoring create.",topic);
			return;
		}
		NewTopic newTopics = new NewTopic(topic, partitionCount.orElse(partitions),replicationFactor.map(e->e.shortValue()).orElse(this.replicationFactor));
		CreateTopicsResult ctr = adminClient.createTopics(Arrays.asList(new NewTopic[]{ newTopics}));
		try {
			ctr.all().get();
			detectedTopics.add(topic);
		} catch (TopicExistsException e) {
			logger.info("Topic: {} already exists, ignoring create",topic);
		} catch (ExecutionException e) {
			logger.error("Error creating topic: "+topic, e);
		} catch (InterruptedException e1) {
			logger.error("Error: ", e1);
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public void delete(String topic) {
		delete(Arrays.asList(topic));
	}

	public void delete(List<String> topics) {
		logger.info("Deleting topics: {}",topics);
		DeleteTopicsResult ctr = adminClient.deleteTopics(topics);
		try {
			ctr.all().get();
			detectedTopics.removeAll(topics);
		} catch (ExecutionException e) {
			logger.error("Error deleting topics: "+topics, e);
		} catch (InterruptedException e) {
			logger.error("Error: ", e);
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public Completable deleteGroups(List<String> groups) {
		return Completable.fromFuture(adminClient.deleteConsumerGroups(groups).all());
	}
	
	@Override
	public Completable deleteCompletable(List<String> topics) {
		DeleteTopicsResult ctr = adminClient.deleteTopics(topics);
		logger.info("Deleting topics: {} result: {}",topics,ctr);
		final KafkaFuture<Void> all = ctr.all();
		return Completable.fromFuture(all)
			.doOnComplete(()->detectedTopics.removeAll(topics))
			.doOnComplete(()->logger.info("Deleted topics: {} complete!",topics));
	}

	
	@Override
	public Subscriber<PubSubMessage> backpressurePublisher(Optional<String> defaultTopic, int maxInFlight) {
		return new DisposableSubscriber<PubSubMessage>() {

			private final Map<String,AtomicLong> logCounter = new HashMap<>();
			private final Map<String,AtomicLong> failCounter = new HashMap<>();

			@Override
			protected void onStart() {
				logger.info("Initial maxinflight: {} items.",maxInFlight);
				request(maxInFlight);
			}

			@Override
			public void onComplete() {
				logger.warn("Kafka producer done.");
				this.dispose();
			}

			@Override
			public void onError(Throwable e) {
				logger.error("Error: ", e);
			}

			@Override
			public void onNext(PubSubMessage msg) {
				if(!msg.topic().isPresent() && !defaultTopic.isPresent()) {
					logger.warn("Missing topic found....");
				}
				String topic = 	msg.topic()
						.orElse(defaultTopic.orElse("DUMPERROR"));
				log(topic,msg);

				KafkaTopicPublisher.this.publish(topic, msg.key(), msg.value(),o->{
					msg.commit();
					request(1);
				},e->{
					logger.error("Error detected enqueuing:",e);
					request(1);
					logProblem(msg,topic,e);
				});
			}

			private void log(String topic, PubSubMessage msg) {
				AtomicLong count = logCounter.computeIfAbsent(topic,tpc->new AtomicLong(0));
				long c = count.incrementAndGet();
				if(c % 1000 == 0) {
					logger.info("Published: {} messages to topic: {}",c,topic);
				}
			}
			
			private void logProblem(PubSubMessage msg, String topic, Throwable t) {
				AtomicLong count = failCounter.computeIfAbsent(topic,tpc->new AtomicLong(0));
				long c = count.incrementAndGet();
				String message = new String(msg.value());
				final String key = msg.key();
				logger.error("Failed publishing message to topic: {} fail count: {} key: {} value: \n{}",topic,c,key,message);
				logger.error("Publish failure",t);
			}
			

		};
	}

	@Override
	public Observable<String> listConsumerGroups() {
		return Single.fromFuture(adminClient.listConsumerGroups().valid())
				.flatMapObservable(Observable::fromIterable)
				.map(ConsumerGroupListing::groupId);
	}

	public Flowable<Map<String,String>> describeConsumerGroups(List<String> groups) {
		return Flowable.fromIterable(adminClient.describeConsumerGroups(groups).describedGroups().values())
			.flatMapSingle(Single::fromFuture)
			.map(this::parseGroupDescription);
	}

	private Map<String,String> parseGroupDescription(ConsumerGroupDescription desc) {
		Map<String,String> result = new HashMap<>();
		result.put("groupId", desc.groupId());
		result.put("state", desc.state().toString());
		result.put("host", desc.coordinator().host());
		result.put("memberCount", ""+desc.members().size());
		return Collections.unmodifiableMap(result);
		
	}
	public Single<Map<String,Long>> consumerGroupOffsets(String groupId) {
		class Tuple {
			public final String name;
			public final long offset;
			public Tuple(String name, long offset) {
				this.name = name;
				this.offset = offset;
			}
		}
		return Single.fromFuture(	adminClient.listConsumerGroupOffsets(groupId)
			.partitionsToOffsetAndMetadata())
			.map(item->item.entrySet().stream().map(e->new Tuple(e.getKey().topic()+"-"+e.getKey().partition(), e.getValue().offset()))
					.collect(Collectors.toMap(e->e.name, e->e.offset))
			);
		
			
			
	}

	@Override
	public void describeTopic(String topic) {
		try {
//			adminClient.describeTopics(Arrays.asList(new String[]{topic})).values().values().stream().findFirst().get().get().partitions().stream().findAny().get().
			final Map<String, TopicDescription> description = adminClient.describeTopics(Arrays.asList(topic),new DescribeTopicsOptions()).all().get();
			logger.info("{}",description);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		} catch (ExecutionException e) {
			logger.error("Error: ", e);
		}
	}
}
