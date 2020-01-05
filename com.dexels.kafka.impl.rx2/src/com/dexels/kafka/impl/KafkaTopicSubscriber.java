package com.dexels.kafka.impl;

import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Dependent;
import javax.inject.Inject;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.kafka.api.KafkaTopicSubscriberConfiguration;
import com.dexels.kafka.api.OffsetQuery;
import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.api.TopicSubscriber;

@Component(name = "navajo.resource.kafkatopicsubscriber", configurationPolicy = ConfigurationPolicy.REQUIRE, immediate = true, property={"event.topics=navajo/shutdown"})
@ApplicationScoped
public class KafkaTopicSubscriber implements PersistentSubscriber,TopicSubscriber, OffsetQuery {
    private final AtomicBoolean doCommit = new AtomicBoolean(false);
    private final AtomicBoolean deactived = new AtomicBoolean(false);
    private Properties defaultProperties;
	private long pollTimeout;
	private Properties configurationProperties;
	private final Set<KafkaConsumer<String,byte[]>> consumers = new HashSet<>();
	private final Map<KafkaConsumer<String, byte[]>,Integer> lastPollCount = new HashMap<>();
	private final static Logger logger = LoggerFactory.getLogger(KafkaTopicSubscriber.class);
	private final Map<KafkaConsumer<String, byte[]>,Long> lastPoll = new HashMap<>();
	private final Map<String,Map<Integer,Long>> offsets = new HashMap<>();

	
	@Activate
	public void activate(Map<String, Object> settings) {
//		this.pollTimeout = Integer.parseInt((String) settings.get("wait"));;
//		configurationProperties.put("max.poll.records", settings.get("max"));

		String bootstrapHosts = (String) settings.get("hosts");
		int maxWaitMillis = Integer.parseInt((String) settings.get("wait"));
		int maxRecordCount = Integer.parseInt((String)settings.get("max"));
		activateConfig(new KafkaTopicSubscriberConfiguration(bootstrapHosts, maxWaitMillis, maxRecordCount));
	}	

	public void activateConfig(KafkaTopicSubscriberConfiguration config) {
		defaultProperties = new Properties();
		defaultProperties.put("bootstrap.servers", config.bootstrapHosts);
		defaultProperties.put("enable.auto.commit", false);
		defaultProperties.put("max.poll.records", "1");
		defaultProperties.put("max.poll.interval.ms", "300000");
		defaultProperties.put("auto.offset.reset", "earliest");
		defaultProperties.put("request.timeout.ms", "30000");
		defaultProperties.put("session.timeout.ms", "20000");
		defaultProperties.put("fetch.max.wait.ms", "20000");		
		defaultProperties.put("key.deserializer", StringDeserializer.class.getCanonicalName());
		defaultProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
		this.pollTimeout = config.maxWaitMillis;
		configurationProperties = new Properties();
		configurationProperties.putAll(defaultProperties);
		configurationProperties.put("max.poll.records", config.maxRecordCount);
	}


	@Deactivate
	public void deactivate() {
		deactived.set(true);
		for (KafkaConsumer<String, byte[]> kafkaConsumer : consumers) {
			closeConsumer(kafkaConsumer);
			
		}
		consumers.clear();
	}

	public void markOffsets(final KafkaConsumer<String, byte[]> cons) {
		Map<String,List<PartitionInfo>> topics = cons.listTopics();
		topics.entrySet().forEach(e->{
			e.getValue().forEach(p->{
			});
		});
	}

	private ConsumerRecords<String, byte[]> pollConsumer(final KafkaConsumer<String, byte[]> cons, boolean allowCommit) throws InterruptException, CommitFailedException {
		if(allowCommit && doCommit.get()) {
			cons.commitSync();
			doCommit.set(false);
		}
		ConsumerRecords<String, byte[]> poll = cons.poll(Duration.ofMillis(pollTimeout));
		lastPollCount.put(cons, poll.count());
		long now = System.currentTimeMillis();
		Long previous = this.lastPoll.get(cons);
//		cons.subscription()
		if(previous!=null && poll.count()!=0) {
			long l = now-previous;
			if(l>1000) {
				logger.info("Time between polls: "+l+" topic: <[not supplied]> size: "+poll.count());
			}
		}
		lastPoll.put(cons, now);
		return poll;
	}

	public Publisher<List<PubSubMessage>> subscribe(List<String> topics,String consumerGroup,Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned) {
		return subscribe(topics, consumerGroup, Optional.empty(), Optional.empty(), clientId, fromBeginning, onPartitionsAssigned);
	}
	
	@Override
	public String encodeTag(Map<String, Map<Integer, Long>> tag) {
		return tag.entrySet()
				.stream()
				.map(e->e.getKey()+"|"+e.getValue().entrySet().stream().map(f->""+f.getKey()+":"+f.getValue()).collect(Collectors.joining(","))
			).collect(Collectors.joining(";"));
	}

	public String encodeTopicTag(Map<Integer, Long> tagMap) {
		return encodeTopicTag(partition->tagMap.get(partition), new ArrayList<>(tagMap.keySet()));
	}
	public String encodeTopicTag(Function<Integer, Long> tag, List<Integer> partitions) {
		return partitions.stream().map(e->e+":"+tag.apply(e)).collect(Collectors.joining(",")); // .entrySet().stream().map(f->""+f.getKey()+":"+f.getValue()).collect(Collectors.joining(","))).collect(Collectors.joining(";"));
	}

	public Function<Integer, Long> decodeTopicTag(String tag) {
		String partitionPairs[] = tag.split(",");
		Map<Integer,Long> pairMap = new HashMap<>();
		for (String pair : partitionPairs) {
			pairMap.put(Integer.parseInt(pair.split(":")[0]), Long.parseLong(pair.split(":")[1]));
		}
		logger.info("Decode Topic Tag: "+pairMap);
		return i->pairMap.get(i);
	}
	
	@Override
	public BiFunction<String, Integer, Long> decodeTag(String tag) {
//		temptopic-345|0:1,1:1;temptopic-123|0:1,1:1
		Map<String, Function<Integer, Long>> result = new HashMap<>();
		String[] topics = tag.split(";");
		for (String tp : topics) {
			String parts[] = tp.split("\\|");
			String topic = parts[0];
			result.put(topic, decodeTopicTag(parts[1]));
		}
		boolean isSingle = tag.indexOf(';')==-1;
		if(isSingle) {
			return (topic,pt)->result.entrySet().stream().findFirst().get().getValue().apply(pt);
		}
		return (topic,partition)->result.get(topic).apply(partition);
	}

	@Override
	public Publisher<List<PubSubMessage>> subscribeSingleRange(String topic, String consumerGroup, String fromTag, String toTag) {
		final Function<Integer, Long> decodedFrom = decodeTopicTag(fromTag);
		final Function<Integer, Long> decodedTo = decodeTopicTag(toTag);
		return subscribe(Arrays.asList(new String[]{topic}), consumerGroup, Optional.of((top,part)->decodedFrom.apply(part)), Optional.of((top,part)->decodedTo.apply(part)), Optional.of("clientid-"+UUID.randomUUID().toString()), false, ()->{});
	}
	
	@Override
	public Publisher<List<PubSubMessage>> subscribe(List<String> topics, String consumerGroup, Optional<BiFunction<String,Integer, Long>> fromTag, Optional<BiFunction<String, Integer, Long>> toTag, Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned, boolean commitBeforeEachPoll) {
		return new Publisher<List<PubSubMessage>>() {
			private Subscription subscription;
			AtomicLong requests = new AtomicLong();
			AtomicBoolean isRunning = new AtomicBoolean(false);

			@Override
			public void subscribe(Subscriber<? super List<PubSubMessage>> sub) {
				final KafkaConsumer<String, byte[]> cons = createConsumer(consumerGroup, clientId,fromBeginning,()->{});
				logger.info("Subscribed to topic: {} with groupId: {} and clientId: {}",topics,consumerGroup,clientId.orElse("<none>"));
				Map<String,Set<Integer>> initialActivePartitions = allPartitions(topics,cons);
				try {
					cons.subscribe(topics,new KafkaConsumerRebalanceListener(cons, consumerGroup, fromBeginning, onPartitionsAssigned, fromTag));
				} catch (WakeupException e) {
		             if (!deactived.get()) throw e;
		        }
				final BiConsumer<TopicPartition,Long> commitConsumer = (topicpartition,offset)->{
					OffsetAndMetadata oam = new OffsetAndMetadata(offset);
					Map<TopicPartition,OffsetAndMetadata> map = new HashMap<>();
					map.put(topicpartition, oam);
					cons.commitAsync(map, (result,exception)->{});
				};
				this.subscription = new Subscription() {
					Map<String,Set<Integer>> activePartitions = initialActivePartitions;
					AtomicBoolean isCompleted = new AtomicBoolean(false);
					@Override
					public void request(long r) {
						Throwable detectedError = null;
						requests.addAndGet(r);
						try {
							
							while(requests.get()>0 && isRunning.get() && detectedError==null && isCompleted.get()==false) {
								requests.decrementAndGet();
								ConsumerRecords<String, byte[]> result;
								try {
									result = pollConsumer(cons,commitBeforeEachPoll);
									doCommit.set(true);
//									logger.info("Consumer records: "+result.count());
									List<PubSubMessage> res = new ArrayList<>(result.count());
									for (ConsumerRecord<String,byte[]> e : result) {
										if(!isRunning.get()) {
											break;
										}
										final KafkaMessage kafkaMsg = new KafkaMessage(e,commitConsumer);
										final Set<Integer> set = activePartitions.get(kafkaMsg.topic().get());
										if(set!=null) {
											if(set.contains(kafkaMsg.partition().get())) {
												res.add(kafkaMsg);
											}
										}
										activePartitions = continueAfter(kafkaMsg, toTag, activePartitions);
										if (activePartitions.isEmpty()) {
											logger.info("No more active partitions for groupId: {}", consumerGroup);
											isRunning.set(false);
										}

										Map<Integer, Long> topicOffsets = offsets.get(e.topic());
										if(topicOffsets==null) {
											topicOffsets = new HashMap<>();
											offsets.put(e.topic(), topicOffsets);
										}
										topicOffsets.put(e.partition(), e.offset());
									}
									sub.onNext(res);
								} catch (InterruptException e1) {
									detectedError = e1;
									logger.error("Interrupted at groupId {} : ", consumerGroup, e1);
									isRunning.set(false);
								} catch(CommitFailedException cfe) {
									sub.onError(cfe);
									logger.error("Commit issue at groupId: {} downstream should re-subscribe.", consumerGroup);
									isRunning.set(false);
								}
							}
						} catch (Throwable e) {
							logger.error("Error in poll for groupId (): ", consumerGroup, e);
							sub.onError(e);
							isRunning.set(false);
							isCompleted.set(true);
							return;
						}
						if(!isRunning.get()) {
							if(!isCompleted.get()) {
								sub.onComplete();
								subscription.cancel();
								isCompleted.set(true);
							}
							return;
						}
					}
					
					@Override
					public void cancel() {
						logger.info("Cancelling subscription for groupId: {}", consumerGroup);
						isRunning.set(false);
					}
				};
				isRunning.set(true);
				sub.onSubscribe(subscription);
			}
		};
	}
	
	protected Map<String, Set<Integer>> allPartitions(List<String> topics, KafkaConsumer<String, byte[]> cons) {
		Map<String, Set<Integer>> result = new HashMap<>();
			topics.forEach(topic->{
				Set<Integer> partitions = cons.partitionsFor(topic)
				.stream()
				.map(p->p.partition())
				.collect(Collectors.toSet());
			result.put(topic, partitions);
			
		});
		return result;
	}


	private Map<String, Set<Integer>> continueAfter(KafkaMessage msg, Optional<BiFunction<String, Integer, Long>> toTag, Map<String,Set<Integer>> activePartitions) {
		if(!toTag.isPresent()) {
			return activePartitions;
		}
		if(!msg.topic().isPresent() || !msg.partition().isPresent() || !msg.offset().isPresent()) {
			throw new RuntimeException("Missing topic, partition or offset in message");
		}

		Long offset = toTag.get().apply(msg.topic().get(), msg.partition().get());
		if(offset==null) {
			logger.info("Weird: No offset found for topic: {} and partition: {}",msg.topic().get(), msg.partition().get());
			return activePartitions;
		}
		if(msg.offset().get()+1 >= offset) {
			logger.info("Message past target offset: {} : topic: {} partition: {} offset: {}",offset, msg.topic().get(), msg.partition().get(), msg.offset().get());
//			partitionCompletedConsumer.accept(msg.topic().get(), msg.partition().get());
			return partitionDone(msg.topic().get(), msg.partition().get(), activePartitions);
		}
		return activePartitions;
	}
	
	private Map<String,Set<Integer>> partitionDone(String topic, int partition, Map<String,Set<Integer>> activePartitions) {
		Set<Integer> activeNow = new HashSet<>(activePartitions.get(topic));
		if(!activeNow.contains(partition)) {
			// already removed?
			return activePartitions;
//			throw new RuntimeException("Deactivating inactive partition: "+activePartitions+" topic: "+topic+" partitions: "+partition);
			
		}
		Map<String,Set<Integer>> result = new HashMap<>(activePartitions);
		activeNow.remove(partition);
		if(activeNow.isEmpty()) {
			result.remove(topic);
			return result;
		}
		result.put(topic, activeNow);
		return result;
	}
	
	private synchronized KafkaConsumer<String, byte[]> createConsumer(String groupId, Optional<String> clientId, boolean withHistory, Runnable onPartitionsAssigned) {
		KafkaConsumer<String, byte[]> consumer;
		logger.info("Creating a consumer groupid: {} clientid: {} with history: {}",groupId,clientId,withHistory);
		final ClassLoader original = Thread.currentThread().getContextClassLoader();
		try {
			Properties copy = new Properties();
			copy.putAll(configurationProperties);
			copy.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			if(clientId.isPresent()) {
				copy.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId.get());
			}
			if(withHistory) {
				copy.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			} else {
				copy.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
			}
			Thread.currentThread().setContextClassLoader(KafkaConsumer.class.getClassLoader());
			consumer = new KafkaConsumer<>(copy);
			consumers.add(consumer);
			
		} finally {
			Thread.currentThread().setContextClassLoader(original);
		}
		return consumer;
	}
	
	private synchronized void closeConsumer(KafkaConsumer<String, byte[]> consumer) {
		logger.info("Closing consumer on thread: {}", Thread.currentThread().getName());
		consumer = null;
	}

	private AtomicLong written = new AtomicLong();
	private static long started = System.currentTimeMillis();
	
	protected void writeToFile(FileOutputStream fo, byte[] a) {
		try {
			long w = written.addAndGet(a.length);
			fo.write(a);
			if(w % 1000==0) {
			    logger.debug("Written: "+written);
				long elapsed = System.currentTimeMillis() - started;
				float rate = (float)w / (float)elapsed / 1024f / 1024f * 1000;
				logger.debug("rate: {} MB/s", rate);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

    private final class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {
        private KafkaConsumer<String, byte[]> consumer;
        private String groupId;
        private boolean withHistory;
        private Runnable onPartitionsAssigned;
		private final Optional<BiFunction<String, Integer, Long>> fromTag;
        
        public KafkaConsumerRebalanceListener(KafkaConsumer<String, byte[]> consumer, String groupId, boolean withHistory, Runnable onPartitionsAssigned,Optional<BiFunction<String,Integer, Long>> fromTag) {
            this.consumer = consumer;
            this.groupId = groupId;
            this.withHistory = withHistory;
            this.onPartitionsAssigned = onPartitionsAssigned;
            this.fromTag = fromTag;
        }
        
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> c) {
            for (TopicPartition tp : c) {
                Long last = lastPoll.get(consumer);
                if(last!=null) {
                    logger.info("Partitions revoked: {} millis after last poll!",(System.currentTimeMillis()- last));
                }
                logger.info("Partition revoked for topic: {} and partition: {}",tp.topic(),tp.partition());
            }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> c) {
            logger.info("Partitions assigned!");
			if(fromTag.isPresent()) {
				BiFunction<String, Integer, Long> from = fromTag.get();
				for (TopicPartition tp : c) {
					Long offset = from.apply(tp.topic(), tp.partition());
					logger.info("GroupId: {} Partition assigned for topic: {} and partition: {} at position: {} seeking to: {}",groupId,tp.topic(),tp.partition(),consumer.position(tp),offset);
					consumer.seek(tp, offset);
				}
            } else {
                if(!withHistory) {
                	logger.info("Not with history");
                    boolean only0 = true;
                    for (TopicPartition tp : c) {
                        long position = consumer.position(tp);
                        logger.info("GroupId: {} Topic: {} Partition: {}  no history selected. Current pos: {}",groupId,tp.topic(),tp.partition(),position);
                        if(position!=0) {
                            only0 = false;
                        }
                    }
                    if (only0) {
                        logger.info("Only position 0 detected for group: {}, so assuming a new topic / group, fast forwarding as we're not interested in history",groupId);
                        consumer.seekToEnd(c);
                        for (TopicPartition tp : c) {
                            logger.info("GroupId: {} Topic: {} Partition: {} fast forwarded. Current pos: {}",groupId,tp.topic(),tp.partition(), consumer.position(tp));
                        }
                        consumer.commitSync();
                    } else {
                        logger.info("Position offsets detected, so won't fast forward group: {}",groupId);
                    }
                }
            }
            onPartitionsAssigned.run();
        }
    }

	@Override
	public Publisher<List<PubSubMessage>> subscribe(String topic, String consumerGroup, boolean fromBeginning) {
		return subscribe(Arrays.asList(new String[]{topic}), consumerGroup, Optional.empty(), fromBeginning, ()->{});
	}


	@Override
	public List<String> topics() {
		KafkaConsumer<String, byte[]> consumer = createConsumer( "offsetquery", Optional.of("offsetquery-"+UUID.randomUUID().toString()), false, ()->{});
		List<String> topics =  consumer.listTopics()
				.entrySet()
				.stream()
				.map(pp->pp.getKey())
				.collect(Collectors.toList());	
		consumer.close();
		return topics;
	}

	public Map<Integer,Long> partitionOffsets(String topic) {
		KafkaConsumer<String, byte[]> consumer = createConsumer("offsetquery", Optional.of("offsetquery-"+UUID.randomUUID().toString()), false, ()->{});
		Map<Integer, Long> offsets = offsetsForTopic(topic, consumer);
		consumer.close();
		consumers.remove(consumer);
		return offsets;
	}


	private Map<Integer, Long> offsetsForTopic(String topic, KafkaConsumer<String, byte[]> consumer) {
		List<TopicPartition> parts =  consumer.partitionsFor(topic)
			.stream()
			.map(pp->new TopicPartition(pp.topic(), pp.partition()))
			.collect(Collectors.toList());
		
		Map<Integer,Long> offsets = consumer.endOffsets(parts)
			.entrySet()
			.stream()
			.collect(
				Collectors.toMap(e->e.getKey().partition(), f->f.getValue().longValue()+1)
			);
		return offsets;
	}


	@Override
	public Map<String, Map<Integer, Long>> offsets(List<String> topics) {
		KafkaConsumer<String, byte[]> consumer = createConsumer( "offsetquery", Optional.of("offsetquery-"+UUID.randomUUID().toString()), false, ()->{});
		return topics.stream()
				.collect(Collectors.toMap(Function.identity(), s->offsetsForTopic(s, consumer)));
	}

}
