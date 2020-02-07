package com.dexels.pubsub.rx2.factory.impl.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dexels.pubsub.rx2.api.PersistentSubscriber;
import com.dexels.pubsub.rx2.api.PubSubMessage;
import com.dexels.pubsub.rx2.factory.impl.BasePubSubMessage;

import io.reactivex.Flowable;

public class KafkaDumpSubscriber implements PersistentSubscriber {

	
	private final static Logger logger = LoggerFactory.getLogger(KafkaDumpSubscriber.class);
	private Reader sourceReader;

	
	public KafkaDumpSubscriber(Reader sourceReader) {
		this.sourceReader = sourceReader;
	}
	
	public Flowable<String> readLines() {
		try {
			AtomicReference<String> lastLine = new AtomicReference<>();
			BufferedReader reader = new BufferedReader(sourceReader);
			lastLine.set(reader.readLine());
			AtomicBoolean hasMore = new AtomicBoolean(true);
			return Flowable.fromIterable(()->new Iterator<String>(){

				@Override
				public boolean hasNext() {
					final boolean b = hasMore.get();
					System.err.println("Calling hasNext: "+b);
					return b;
				}

				@Override
				public String next() {
					String prevLast = lastLine.get();
					if(prevLast==null) {
						logger.warn("Null line detected, should not happen");
						hasMore.set(false);
						return null;
					}
					try {
						final String s = reader.readLine();
						lastLine.set(s);
						if(s==null) {
							hasMore.set(false);
							reader.close();
						}
						return prevLast;
					} catch (IOException e) {
						throw new RuntimeException("Wrapped IOException",e);
					}
				}});
		} catch (IOException e) {
			return Flowable.error(e);
		}
	}
	
	public PubSubMessage parseLine(String input) {
		int i = input.indexOf('\t');
		System.err.println("Type: "+i);
		String[] parts = input.split("\t");
		if(parts.length==1) {
			throw new RuntimeException("Missing tab delimited keys in line");
		}
		if(parts.length>2) {
			throw new RuntimeException("Rogue tabs detected in body");
		}
		byte[] value = "null".equals(parts[1]) ? null : parts[1].getBytes(); 
		return new BasePubSubMessage(parts[0], value, Optional.empty(), System.currentTimeMillis());
	}
	
	@Override
	public Publisher<List<PubSubMessage>> subscribe(List<String> topics, String consumerGroup,
			Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned) {
		logger.warn("Using dumpsubscriber: Topics will be ignored");
		return readLines().map(e->Arrays.asList(new PubSubMessage[]{parseLine(e)}));
	}

	@Override
	public Publisher<List<PubSubMessage>> subscribe(List<String> topics, String consumerGroup,
			Optional<BiFunction<String, Integer, Long>> fromTag, Optional<BiFunction<String, Integer, Long>> toTag,
			Optional<String> clientId, boolean fromBeginning, Runnable onPartitionsAssigned, boolean beforePollCommit) {
		logger.warn("Using dumpsubscriber: Topics will be ignored");
		return readLines().map(e->Arrays.asList(new PubSubMessage[]{parseLine(e)}));
	}

	@Override
	public Publisher<List<PubSubMessage>> subscribeSingleRange(String topic, String consumerGroup, String fromTag,
			String toTag) {
		logger.warn("Using dumpsubscriber: Topics will be ignored");
		return readLines().map(e->Arrays.asList(new PubSubMessage[]{parseLine(e)}));
	}

	@Override
	public Publisher<List<PubSubMessage>> subscribe(String topic, String consumerGroup, boolean fromBeginning) {
		logger.warn("Using dumpsubscriber: Topics will be ignored");
		return readLines().map(e->Arrays.asList(new PubSubMessage[]{parseLine(e)}));
	}

	@Override
	public String encodeTopicTag(Map<Integer, Long> offsetMapInc) {
		return null;
	}

	@Override
	public Map<Integer, Long> partitionOffsets(String topic) {
		return null;
	}

}
